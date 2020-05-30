from typing import NamedTuple, Dict, List, Set, Optional
from itertools import groupby
from dagster import solid, SolidExecutionContext, InputDefinition, usable_as_dagster_type
from pandas import DataFrame, Series
from azmeta.access.specifications import AzureComputeSpecifications, VirtualMachineSku
from .utilization import UtilizationDataFrame
from .resources import ResourcesDataFrame
import functools

@usable_as_dagster_type
class RightSizeAnalysis(NamedTuple):
    advisor_sku: str
    advisor_sku_valid: bool
    advisor_sku_invalid_reason: Optional[str] = None
    pass


@solid(input_defs=[
    InputDefinition('cpu_utilization', UtilizationDataFrame),
    InputDefinition('mem_utilization', UtilizationDataFrame),
    InputDefinition('disk_utilization', UtilizationDataFrame),
    InputDefinition('compute_specs', AzureComputeSpecifications),
    InputDefinition('advisor_recommendations', Dict[str,str]),
    InputDefinition('resources', ResourcesDataFrame),
])
def right_size_engine(context: SolidExecutionContext, 
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
        advisor_sku = advisor_recommendations.get(resource_id)
        valid = None
        reason = None
        if advisor_sku is not None:
            try:
                cpu_data = cpu_utilization.loc[resource_id]
            except KeyError:
                continue
            vm_sku = compute_specs.virtual_machine_by_name(advisor_sku)
            valid = evaluate_cpu_fitness(cpu_data, vm_sku) 
            if valid:
                is_database = resource.role_code == 'DBS'
                mem_data = mem_utilization.loc[resource_id]
                disk_data = disk_utilization.loc[resource_id]
                should_flex_down = is_database and evaluate_low_cached_usage(disk_data, vm_sku)
                valid = evaluate_mem_fitness(mem_data, vm_sku, flex_down=should_flex_down)
                if valid:
                    valid = evaluate_disk_fitness(disk_data, vm_sku)
                    if not valid:
                        reason = "Disk fitness."
                else:
                    reason = "Memory fitness."
            else:
                reason = "CPU fitness"
            results[resource_id] = RightSizeAnalysis(advisor_sku, valid, reason)

    return results


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