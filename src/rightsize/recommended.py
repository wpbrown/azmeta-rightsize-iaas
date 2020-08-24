from typing import Dict
from dagster import solid, SolidExecutionContext, Field, Array, String
from azmeta.access.advisor import load_resize_recommendations


@solid(config_schema={
    'subscriptions': Field(Array(String), description='The subscriptions to query in Azure Advisor.')
})
def get_recommendations(context: SolidExecutionContext) -> Dict[str,str]:
    config = context.solid_config
    recommendations = load_resize_recommendations(config['subscriptions'])
    return {k:v.extended_properties['targetSku'] for k, v in recommendations.items()}