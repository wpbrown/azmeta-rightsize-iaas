from dagster import solid, SolidExecutionContext, InputDefinition, OutputDefinition, Materialization, Output, EventMetadataEntry, Nothing, FileHandle
from dagster.utils import script_relative_path
import dagstermill as dm
from typing import Dict
from pandas import DataFrame
from .right_size import RightSizeAnalysis
from .resources import ResourcesDataFrame
from .utilization import UtilizationDataFrame
from azmeta.access.specifications import AzureComputeSpecifications
from azmeta.access.reporting import convert_nodebook_node_to_html
import nbformat
import json
import os

@solid(input_defs=[
    InputDefinition('analysis', Dict[str, RightSizeAnalysis]),
    InputDefinition('resources', ResourcesDataFrame),
])
def write_operation_inventory(context: SolidExecutionContext, 
    analysis: Dict[str, RightSizeAnalysis],
    resources: DataFrame) -> Nothing:
    resources = resources.set_index('resource_id')
    resizes = [{'subscription_id': resources.at[resource_id, 'subscription_id'], 'resource_id': resource_id, 'current_sku': resources.at[resource_id, 'vm_size'], 'new_sku': analysis.advisor_sku} 
               for resource_id, analysis in analysis.items() if analysis.advisor_sku_valid]
    output = { 'vm_resize_operations': resizes }

    output_path = os.path.abspath(f'operation_inventory_{context.run_id}.json')
    with open(output_path, 'w') as fd:
        json.dump(output, fd, indent=3)

    yield Materialization(
        label='operation_inventory',
        description='An inventory of the right sizing operations that are recommended and validated.',
        metadata_entries=[
            EventMetadataEntry.path(
                output_path, 'operation_inventory_path'
            )
        ],
    )
    yield Output(None)


right_size_report = dm.define_dagstermill_solid(
    'right_size_report', script_relative_path('rightsizereport.ipynb'),
    input_defs=[
        InputDefinition('advisor_analysis', Dict[str, RightSizeAnalysis]),
        InputDefinition('local_analysis', Dict[str, RightSizeAnalysis]),
        InputDefinition('cpu_utilization', UtilizationDataFrame),
        InputDefinition('mem_utilization', UtilizationDataFrame),
        InputDefinition('disk_utilization', UtilizationDataFrame),
        InputDefinition('compute_specs', AzureComputeSpecifications),
        InputDefinition('resources', ResourcesDataFrame),
    ], output_notebook='output_notebook'
)


@solid(input_defs=[
    InputDefinition('report_notebook', FileHandle)
])
def write_html_report(context: SolidExecutionContext, report_notebook: FileHandle) -> Nothing:
    with context.file_manager.read(report_notebook) as node_file:
        node = nbformat.read(node_file, nbformat.NO_CONVERT)
    html = convert_nodebook_node_to_html(node, full_width=True)
    handle = context.file_manager.write_data(html.encode(), ext='html')
    yield Materialization(
        label='resize_report',
        description='A report of all VMs utilization data and evaulation of the recommendations.',
        metadata_entries=[
            EventMetadataEntry.path(handle.path_desc, 'resize_report_path')
        ],
    )
