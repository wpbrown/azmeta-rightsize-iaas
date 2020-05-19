from dagster import pipeline, ModeDefinition
from rightsize.resources import query_vm_resources
from rightsize.utilization import (
    query_cpu_utilization, normalize_cpu_utilization,
    query_mem_utilization, normalize_mem_utilization,
    query_disk_utilization, normalize_disk_utilization,
    default_azure_monitor_context
)
from rightsize.specifications import load_compute_specs
from rightsize.recommended import get_recommendations
from rightsize.right_size import right_size_engine
from rightsize.output import write_operation_inventory, right_size_report, write_html_report

@pipeline(
    mode_defs=[ModeDefinition(
        resource_defs={'azure_monitor': default_azure_monitor_context}
    )]
)
def rightsize_pipeline():
    vm_resources = query_vm_resources()
    cpu_utilization = query_cpu_utilization(vm_resources)
    mem_utilization = query_mem_utilization(vm_resources)
    disk_utilization = query_disk_utilization(vm_resources)

    compute_specs = load_compute_specs()
    cpu_utilization = normalize_cpu_utilization(utilization=cpu_utilization, compute_specs=compute_specs, resources=vm_resources)
    mem_utilization = normalize_mem_utilization(utilization=mem_utilization, compute_specs=compute_specs, resources=vm_resources)
    disk_utilization = normalize_disk_utilization(utilization=disk_utilization, compute_specs=compute_specs, resources=vm_resources)

    recommendations = get_recommendations()
    right_size_analysis = right_size_engine(cpu_utilization=cpu_utilization, mem_utilization=mem_utilization, disk_utilization=disk_utilization, 
                                            compute_specs=compute_specs, advisor_recommendations=recommendations, resources=vm_resources)
    
    write_operation_inventory(analysis=right_size_analysis, resources=vm_resources)
    report_notebook = right_size_report(analysis=right_size_analysis, resources=vm_resources, compute_specs=compute_specs,
                                        cpu_utilization=cpu_utilization, mem_utilization=mem_utilization, disk_utilization=disk_utilization)
    write_html_report(report_notebook=report_notebook)