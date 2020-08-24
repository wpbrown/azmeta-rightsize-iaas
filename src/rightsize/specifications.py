from dagster import solid, SolidExecutionContext, Field, Array, String, PythonObjectDagsterType, make_python_type_usable_as_dagster_type
from typing import Any, Optional, List, TYPE_CHECKING
from azmeta.access.specifications import AzureComputeSpecifications, load_compute_specifications


AzureComputeSpecificationsDagsterType = PythonObjectDagsterType(AzureComputeSpecifications)
make_python_type_usable_as_dagster_type(AzureComputeSpecifications, AzureComputeSpecificationsDagsterType)


@solid(config_schema={
    'subscription': Field(String, is_required=False, description='The subscription ID to list SKUs from.')
})
def load_compute_specs(context: SolidExecutionContext) -> AzureComputeSpecifications:
    return load_compute_specifications(logger=context.log)

