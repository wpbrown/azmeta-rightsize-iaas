from dagster import solid, SolidExecutionContext, Field, Array, String
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from pandas import DataFrame
from typing import Any, Optional, List, TYPE_CHECKING
from azmeta.access.resource_graph import query_dataframe

if TYPE_CHECKING:
    ResourcesDataFrame = Any # DataFrame # Pandas has no type info yet.
else:
    ResourcesDataFrame = create_dagster_pandas_dataframe_type(
        name='ResourcesDataFrame',
        columns=[
            PandasColumn.string_column('resource_id'),
            PandasColumn.string_column('subscription_id'),
        ],
    ) 


@solid(config={
    'subscriptions': Field(Array(String), description='The subscriptions to query in the Resource Graph.'),
    'filters': Field(String, is_required=False, description='Conditions for a KQL where operator.'),
    'custom_projections': Field(String, is_required=False, description='Assignments for a KQL project operator.'),
})
def query_vm_resources(context: SolidExecutionContext) -> ResourcesDataFrame:
    config = context.solid_config
    filters = f'| where {config["filters"]}' if 'filters' in config else ''
    custom_projections = f', {config["custom_projections"]}' if 'custom_projections' in config else ''
    
    query = f"Resources | where type =~ 'Microsoft.Compute/virtualMachines' {filters} | project resource_id = tolower(id), subscription_id = subscriptionId {custom_projections}"

    dataframe_result = query_dataframe(config['subscriptions'], query)
    
    return dataframe_result

