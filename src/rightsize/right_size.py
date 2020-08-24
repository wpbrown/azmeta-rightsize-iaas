from typing import NamedTuple, Dict, List, Set, Optional
from itertools import groupby
from dagster import solid, SolidExecutionContext, InputDefinition, usable_as_dagster_type
from pandas import DataFrame, Series
import pandas
from azmeta.access.specifications import AzureComputeSpecifications, VirtualMachineSku
from azmeta.access.utils.math import idivceil
from .utilization import UtilizationDataFrame
from .resources import ResourcesDataFrame
import functools

@usable_as_dagster_type
class RightSizeAnalysis(NamedTuple):
    advisor_sku: str
    advisor_sku_valid: bool
    advisor_sku_invalid_reason: Optional[str] = None
    annual_savings_no_ri: Optional[float] = None


class Fitness(NamedTuple):
    cpu: bool
    memory: bool
    disk: bool


class VmData(NamedTuple):
    cpu: Series
    memory: Series
    disk: DataFrame


@solid(input_defs=[
    InputDefinition('cpu_utilization', UtilizationDataFrame),
    InputDefinition('mem_utilization', UtilizationDataFrame),
    InputDefinition('disk_utilization', UtilizationDataFrame),
    InputDefinition('compute_specs', AzureComputeSpecifications),
    InputDefinition('advisor_recommendations', Dict[str,str]),
    InputDefinition('resources', ResourcesDataFrame),
])
def advisor_validator(context: SolidExecutionContext, 
    cpu_utilization: DataFrame,
    mem_utilization: DataFrame,
    disk_utilization: DataFrame,
    compute_specs: AzureComputeSpecifications, 
    advisor_recommendations: Dict[str, str],
    resources: DataFrame) -> Dict[str, RightSizeAnalysis]:
    cpu_utilization = cpu_utilization.set_index('resource_id')
    mem_utilization = mem_utilization.set_index('resource_id')
    disk_utilization = disk_utilization.set_index('resource_id').sort_index()
    results: Dict[str, RightSizeAnalysis] = {}
    for resource in resources.itertuples():
        resource_id = resource.resource_id
        is_database = resource.role_code == 'DBS'
        advisor_sku = advisor_recommendations.get(resource_id)
        if advisor_sku is None:
            continue
        data = select_vm_data(resource_id, cpu_utilization, mem_utilization, disk_utilization)
        if data is None:
            continue
        test_sku = compute_specs.virtual_machine_by_name(advisor_sku)
        should_flex_mem_down = is_database and evaluate_low_cached_usage(data.disk, test_sku)
        fitness = evaluate_overall_fitness(test_sku, data, should_flex_mem_down)

        valid = fitness.cpu and fitness.memory and fitness.disk
        reason = None if valid else f"{'CPU ' if not fitness.cpu else ''}{'Memory ' if not fitness.memory else ''}{'I/O ' if not fitness.disk else ''}fitness."
        results[resource_id] = RightSizeAnalysis(test_sku.name, valid, reason)

    return results


@solid(input_defs=[
    InputDefinition('cpu_utilization', UtilizationDataFrame),
    InputDefinition('mem_utilization', UtilizationDataFrame),
    InputDefinition('disk_utilization', UtilizationDataFrame),
    InputDefinition('compute_specs', AzureComputeSpecifications),
    InputDefinition('resources', ResourcesDataFrame),
])
def right_size_engine(context: SolidExecutionContext, 
    cpu_utilization: DataFrame,
    mem_utilization: DataFrame,
    disk_utilization: DataFrame,
    compute_specs: AzureComputeSpecifications, 
    resources: DataFrame) -> Dict[str, RightSizeAnalysis]:
    cpu_utilization = cpu_utilization.set_index('resource_id')
    mem_utilization = mem_utilization.set_index('resource_id')
    disk_utilization = disk_utilization.set_index('resource_id').sort_index()

    annual_sql_2core_cost = 10.0
    annual_win_server = 10.0
    location = "eastus2"
    prices = pandas.read_json('prices202006.eastus2.json')
    
    def find_vm_billables(vm_size: str):
        s = compute_specs.virtual_machine_by_name(vm_size)
        bill_sku = s.capabilities.parent_size if s.capabilities.parent_size else vm_size
        bill_sku = bill_sku.replace('s_', '_').replace('_DS', '_D')
        cores = s.capabilities.d_vcpus_available
        return (bill_sku, cores)

    def find_vm_record(vm_size: str, payg: bool):
        nonlocal location
        parts = prices[(prices.armSkuName == vm_size) & (prices.armRegionName == location) & (prices.type == 'Consumption') & (prices.serviceName == 'Virtual Machines') & ~pandas.isna(prices.partNumber) & ~prices.skuName.str.contains('Low Priority')]
        if parts.shape[0] != 2:
            print(f'failed to locate price for {vm_size}')
            return None
        if payg:
            record = parts[parts.productName.str.endswith('Windows')].iloc[0]
        else:
            record = parts[~parts.productName.str.endswith('Windows')].iloc[0]
        return record

    def price_sku(sku: VirtualMachineSku): 
        billing_sku_name, billing_cores = find_vm_billables(sku.name)
        vm_record = find_vm_record(billing_sku_name, payg=False)
        if vm_record is None:
            return None
        if vm_record.unitOfMeasure != '1 Hour':
            raise 'Unhandled UOM'
        sql_cost = max(4,billing_cores) / 2 * annual_sql_2core_cost
        win_cost = (0.5 if billing_cores <= 8 else float(idivceil(billing_cores, 16))) * annual_win_server
        vm_cost = vm_record.unitPrice * 24 * 365 
        return sql_cost + vm_cost + win_cost

    skus = ((price_sku(s), s) for s in compute_specs.virtual_machine_skus if s.family.startswith('standardD') or s.family.startswith('standardES') or s.family.startswith('standardMS'))
    skus_hash = {s[1].name.lower():s for s in skus if s[0] is not None}
    
    new_sku_families = {'standardDSv2Family', 'standardDSv3Family', 'standardESv3Family', 'standardMSFamily'}
    new_skus = (s for s in skus_hash.values() if s[1].family in new_sku_families and not (s[1].capabilities.d_vcpus_available < 4 and s[1].capabilities.vcpus > s[1].capabilities.d_vcpus_available))
    new_skus_list = sorted(new_skus, key=lambda x:x[0])

    results: Dict[str, RightSizeAnalysis] = {}
    for resource in resources.itertuples():
        resource_id = resource.resource_id
        is_database = resource.role_code == 'DBS'
        data = select_vm_data(resource_id, cpu_utilization, mem_utilization, disk_utilization)
        if data is None:
            continue
        
        vm_size = resource.vm_size.lower()
        sku_current_cost, sku_current = skus_hash[vm_size]
        
        for test_cost, test_sku in new_skus_list:
            if test_cost > sku_current_cost:
                break

            should_flex_mem_down = is_database and evaluate_low_cached_usage(data.disk, test_sku)
            fitness = evaluate_overall_fitness(test_sku, data, should_flex_mem_down)
            if fitness.cpu and fitness.memory and fitness.disk:
                break
            
            mem_equity = test_sku.capabilities.memory_gb == sku_current.capabilities.memory_gb
            if fitness.cpu and fitness.disk and mem_equity:
                break

        
        if resource.vm_size == test_sku.name:
            analysis = RightSizeAnalysis(test_sku.name, False, "Reduction not possible.")
        elif test_cost <= sku_current_cost:
            savings = sku_current_cost - test_cost
            analysis = RightSizeAnalysis(test_sku.name, True, None, savings)
        else:
            reason = f"{'CPU ' if not fitness.cpu else ''}{'Memory ' if not fitness.memory else ''}{'I/O ' if not fitness.disk else ''} suggests increase."
            analysis = RightSizeAnalysis(resource.vm_size, False, reason) 
        results[resource_id] = analysis

    return results


def select_vm_data(resource_id: str, cpu_data: DataFrame, mem_data: DataFrame, disk_data: DataFrame) -> Optional[VmData]:
    try:
        resource_cpu_data: Series = cpu_data.loc[resource_id]
        resource_mem_data: Series = mem_data.loc[resource_id]
        resource_disk_data: DataFrame = disk_data.loc[resource_id]
    except KeyError:
        return None
    return VmData(cpu=resource_cpu_data, memory=resource_mem_data, disk=resource_disk_data)

def evaluate_overall_fitness(vm_sku: VirtualMachineSku, data: VmData, should_flex_mem_down: bool) -> Fitness:
    return Fitness(
        cpu=evaluate_cpu_fitness(data.cpu, vm_sku),
        memory=evaluate_mem_fitness(data.memory, vm_sku, flex_down=should_flex_mem_down),
        disk=evaluate_disk_fitness(data.disk, vm_sku))


def evaluate_cpu_fitness(cpu_utilization: Series, vm_sku: VirtualMachineSku) -> bool:
    total_acu = vm_sku.capabilities.d_total_acus
    p95_acu = cpu_utilization.percentile_95th
    p99_acu = cpu_utilization.percentile_99th
    return total_acu > p99_acu * 0.9 and total_acu > p95_acu


def evaluate_mem_fitness(mem_utilization: Series, vm_sku: VirtualMachineSku, flex_down: bool) -> bool:
    total_mem = vm_sku.capabilities.memory_gb * 1024
    p80_mem = mem_utilization.percentile_80th
    p95_mem = mem_utilization.percentile_95th
    p99_mem = mem_utilization.percentile_99th
    if flex_down:
        return total_mem > p99_mem * 0.75 and total_mem > p95_mem * 0.8 and total_mem > p80_mem * 0.8
    else:
        return total_mem > p99_mem * 1.05 and total_mem > p80_mem * 1.10


def evaluate_disk_fitness(disk_utilization: DataFrame, vm_sku: VirtualMachineSku) -> bool:
    groups = disk_utilization.set_index(['cached', 'counter_name'])
    def check_percentile(percentile_name: str, factor: float):
        return (vm_sku.capabilities.combined_temp_disk_and_cached_read_bytes_per_second > groups.at[(True, 'Disk Bytes/sec'), percentile_name] * factor and 
                vm_sku.capabilities.combined_temp_disk_and_cached_iops > groups.at[(True, 'Disk Transfers/sec'), percentile_name] * factor and 
                vm_sku.capabilities.uncached_disk_bytes_per_second > groups.at[(False, 'Disk Bytes/sec'), percentile_name] * factor and 
                vm_sku.capabilities.uncached_disk_iops > groups.at[(False, 'Disk Transfers/sec'), percentile_name] * factor)
    
    return check_percentile('percentile_99th', 0.9) and check_percentile('percentile_95th', 1.0)


def evaluate_low_cached_usage(disk_utilization: DataFrame, vm_sku: VirtualMachineSku) -> bool:
    groups = disk_utilization.set_index(['cached', 'counter_name'])
    def check_percentile(percentile_name: str):
        return (30 * 1024**2 > groups.at[(True, 'Disk Bytes/sec'), percentile_name] and 
                800 > groups.at[(True, 'Disk Transfers/sec'), percentile_name])
    
    return check_percentile('percentile_99th')


#get_family = lambda x:x.family
#spec_families: Dict[str,List[VirtualMachineSku]] = {f[0]:list(f[1]) for f in groupby(sorted(compute_specs.virtual_machine_skus, key=get_family), key=get_family)}