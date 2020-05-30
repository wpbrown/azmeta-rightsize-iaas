from dagster import solid, resource, SolidExecutionContext, ExpectationResult, EventMetadataEntry, Output, InitResourceContext, Field, String, Permissive
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from pandas import DataFrame
import pandas as pd
from typing import Any, Optional, List, TYPE_CHECKING, NamedTuple, Callable, Dict, Set
import dagster_pandas
import functools
from azmeta.access.monitor_logs import (
    PerformanceCounterSpec,
    query_dataframe_by_workspace_chunk, 
    build_perf_counter_percentile_query,
    build_disk_percentile_query
)
from azmeta.access.utils.chunking import build_grouped_chunk_list
from .resources import ResourcesDataFrame
from .specifications import AzureComputeSpecifications

if TYPE_CHECKING:
    UtilizationDataFrame = Any # DataFrame # Pandas has no type info yet.
else:
    UtilizationDataFrame = create_dagster_pandas_dataframe_type(
        name='UtilizationDataFrame',
        columns=[
            PandasColumn.string_column('resource_id'),
            PandasColumn.float_column('percentile_50th'),
            PandasColumn.float_column('percentile_80th'),
            PandasColumn.float_column('percentile_90th'),
            PandasColumn.float_column('percentile_95th'),
            PandasColumn.float_column('percentile_99th'),
            PandasColumn.float_column('max'),
            PandasColumn.integer_column('samples'),
        ],
    ) 


@solid(required_resource_keys={'azure_monitor'})
def query_cpu_utilization(context: SolidExecutionContext, resources: ResourcesDataFrame) -> UtilizationDataFrame:
    builder = functools.partial(build_perf_counter_percentile_query, spec=PerformanceCounterSpec("Processor", "% Processor Time", "_Total"))
    result = _run_query(builder, resources, context.resources.azure_monitor, context.log)
    yield _expect_all_resources_in_result(resources, result)
    yield Output(result)


@solid(required_resource_keys={'azure_monitor'})
def query_mem_utilization(context: SolidExecutionContext, resources: ResourcesDataFrame) -> UtilizationDataFrame:
    builder = functools.partial(build_perf_counter_percentile_query, spec=PerformanceCounterSpec("Memory", "Available Mbytes", None, "value * -1"))
    result = _run_query(builder, resources, context.resources.azure_monitor, context.log)
    yield _expect_all_resources_in_result(resources, result)
    yield Output(result)


@solid(required_resource_keys={'azure_monitor'})
def query_disk_utilization(context: SolidExecutionContext, resources: ResourcesDataFrame) -> UtilizationDataFrame:
    result = _run_query(build_disk_percentile_query, resources, context.resources.azure_monitor, context.log)
    yield _expect_all_resources_in_result(resources, result)
    yield Output(result)


@solid
def normalize_cpu_utilization(context: SolidExecutionContext, utilization: UtilizationDataFrame, compute_specs: AzureComputeSpecifications, resources: ResourcesDataFrame) -> UtilizationDataFrame:
    resources = resources.set_index('resource_id')

    def to_acus(row):
        vm_sku_name = resources.loc[row['resource_id']].vm_size
        vm_sku = compute_specs.virtual_machine_by_name(vm_sku_name)
        acus = vm_sku.capabilities.d_total_acus
        row.update(row['percentile_50th':'max'] / 100 * acus)
        return row

    utilization = utilization.apply(to_acus, axis=1)

    yield Output(utilization)


@solid
def normalize_mem_utilization(context: SolidExecutionContext, utilization: UtilizationDataFrame, compute_specs: AzureComputeSpecifications, resources: ResourcesDataFrame) -> UtilizationDataFrame:
    resources = resources.set_index('resource_id')

    def to_used(row):
        vm_sku_name = resources.loc[row['resource_id']].vm_size
        vm_sku = compute_specs.virtual_machine_by_name(vm_sku_name)
        total_memory_mib = vm_sku.capabilities.memory_gb * 1024
        row.update(total_memory_mib + row['percentile_50th':'max'])
        return row

    utilization = utilization.apply(to_used, axis=1)

    yield Output(utilization)


@solid
def normalize_disk_utilization(context: SolidExecutionContext, utilization: UtilizationDataFrame, compute_specs: AzureComputeSpecifications, resources: ResourcesDataFrame) -> UtilizationDataFrame:
    resources = resources.set_index('resource_id')
    utilization = utilization.set_index('resource_id').sort_index()

    clean_name = lambda name:name.rstrip(':').upper()
    resource_frames = []
    utilization_resources = utilization.groupby(['resource_id'])
    for resource_id, data in utilization_resources:
        all_names = {clean_name(x) for x in data.instance_name.unique()}
        storage_profile = resources.loc[resource_id].storage_profile
        mapping_cache = {x: _disk_is_cached(x, storage_profile, all_names) for x in all_names}
        unmapped = [k for k,v in mapping_cache.items() if v is None]
        if unmapped:
            context.log.warning(f'Failed to deduce cache config for {",".join(unmapped)} on {resource_id}')
            for k in unmapped:
                mapping_cache[k] = True
        data = data.assign(cached=data.apply(lambda x:mapping_cache[clean_name(x.instance_name)], axis=1))
        data = data.groupby(['cached', 'counter_name']).sum().reset_index().assign(resource_id=resource_id)
        resource_frames.append(data)

    unioned = pd.concat(resource_frames, ignore_index=True)
    yield Output(unioned)


def _disk_is_cached(name: str, storage_profile: Dict, all_names: Set[str]) -> Optional[bool]:
    if name == 'D':
        return True

    if name == 'C':
        return _caching_on(storage_profile['osDisk']['caching'])

    if len(all_names) == 3 and len(storage_profile['dataDisks']) == 1:
        return _caching_on(storage_profile['dataDisks'][0]['caching'])
    
    if 'S' in all_names and 'L' in all_names and name in ('S', 'L'):
        suffix = 'data' if name == 'S' else 'log'
        caching = list(x['caching'] for x in storage_profile['dataDisks'] if x['managedDisk']['id'].endswith(suffix))
        if caching:
            return _caching_on(caching[0])

    if name == 'T':
        caching = list(x['caching'] for x in storage_profile['dataDisks'] if x['managedDisk']['id'].endswith('temp'))
        if caching:
            return _caching_on(caching[0])

    cache_settings = set(_caching_on(x['caching']) for x in storage_profile['dataDisks'])
    if len(cache_settings) == 1:
        return next(iter(cache_settings))

    return None


def _caching_on(value: str) -> bool:
    return value.lower() in ('readonly', 'readwrite')


class AzureMonitorContext(object):
    def __init__(self, lookback_duration: str, workspace_map: dict):
        self.lookback_duration = lookback_duration
        self._workspace_map = workspace_map
    
    def map_to_workspace(self, subscription_id: str) -> str:
        return self._workspace_map[subscription_id]


@resource(config={
    'lookback_duration': Field(String, default_value='P30D', is_required=False),
    'workspace_map': Field(Permissive(), is_required=False),
    'workspace': Field(String, is_required=False)
})
def default_azure_monitor_context(context: InitResourceContext):
    if 'workspace_map' not in context.resource_config and 'workspace' not in context.resource_config:
        raise Exception('omg') # TODO add logic

    return AzureMonitorContext(context.resource_config['lookback_duration'], context.resource_config['workspace_map'])


def _expect_all_resources_in_result(resources: ResourcesDataFrame, result: UtilizationDataFrame) -> ExpectationResult:
    input_ids = set(resources.resource_id)
    output_ids = set(result.resource_id)
    missing_ids = input_ids - output_ids
    missing_count = len(missing_ids)

    entries = [EventMetadataEntry.json({'input': len(input_ids), 'output': len(output_ids), 'missing': missing_count}, label='Summary Counts')]
    if missing_count > 0:
        entries.append(EventMetadataEntry.json({'ids': list(missing_ids)}, label='Missing Resources'))

    return ExpectationResult(
        success=(missing_count == 0),
        label='Found All Resources',
        description='Check if all the input resource ids were found in the Azure Monitor Logs workspace.',
        metadata_entries=entries)


def _run_query(builder: Callable, resources: ResourcesDataFrame, context: AzureMonitorContext, logger) -> DataFrame:
    chunk_list = build_grouped_chunk_list(resources.itertuples(), lambda x:x.resource_id, lambda x:context.map_to_workspace(x.subscription_id), chunk_size=32)
    result = query_dataframe_by_workspace_chunk(chunk_list, builder, timespan=context.lookback_duration, logger=logger)
    return result.primary_result
