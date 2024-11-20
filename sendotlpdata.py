from google.protobuf.timestamp_pb2 import Timestamp
from opentelemetry.proto.logs.v1.logs_pb2 import ResourceLogs, ScopeLogs, LogRecord
from opentelemetry.proto.common.v1.common_pb2 import KeyValue, AnyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.common.v1.common_pb2 import InstrumentationScope
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.metrics.v1.metrics_pb2 import ResourceMetrics, ScopeMetrics, Metric, Gauge, NumberDataPoint
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
import time
import base64
import requests
import os
import random

# Define our services and their characteristics
SERVICES = {
    'payment': {
        'name': 'payment-service',
        'operations': ['process_payment', 'validate_card', 'check_fraud', 'authorize_payment'],
        'response_times': (50, 500),  # min/max ms
        'error_rate': 0.05
    },
    'inventory': {
        'name': 'inventory-service',
        'operations': ['check_stock', 'reserve_item', 'update_inventory'],
        'response_times': (20, 200),
        'error_rate': 0.02
    },
    'shipping': {
        'name': 'shipping-service',
        'operations': ['calculate_rates', 'create_label', 'schedule_pickup'],
        'response_times': (100, 1000),
        'error_rate': 0.03
    },
    'notification': {
        'name': 'notification-service',
        'operations': ['send_email', 'send_sms', 'push_notification'],
        'response_times': (10, 100),
        'error_rate': 0.01
    },
    'order': {
        'name': 'order-service',
        'operations': ['create_order', 'update_order', 'cancel_order'],
        'response_times': (30, 300),
        'error_rate': 0.04
    }
}

HOSTS = {
    'host-1': {'cpu_cores': 4, 'memory': 8192},
    'host-2': {'cpu_cores': 8, 'memory': 16384},
    'host-3': {'cpu_cores': 4, 'memory': 8192},
    'host-4': {'cpu_cores': 16, 'memory': 32768}
}

def generate_w3c_trace_id():
    random_bytes = os.urandom(16)
    return random_bytes.hex()

def generate_w3c_span_id():
    random_bytes = os.urandom(8)
    return random_bytes.hex()

def create_span(trace_id_bytes, parent_span_id_bytes, service, operation, start_time_nano):
    span_id = generate_w3c_span_id()
    span_id_bytes = bytes.fromhex(span_id)
    
    # Generate random duration based on service characteristics
    duration = random.randint(
        SERVICES[service]['response_times'][0] * 1000000,  # Convert ms to ns
        SERVICES[service]['response_times'][1] * 1000000
    )
    
    # Randomly generate error based on service error rate
    status_code = 'ERROR' if random.random() < SERVICES[service]['error_rate'] else 'OK'
    
    span = Span(
        trace_id=trace_id_bytes,
        span_id=span_id_bytes,
        parent_span_id=parent_span_id_bytes if parent_span_id_bytes else None,
        name=operation,
        kind=Span.SpanKind.SPAN_KIND_SERVER,
        start_time_unix_nano=start_time_nano,
        end_time_unix_nano=start_time_nano + duration,
        attributes=[
            KeyValue(
                key="service.name",
                value=AnyValue(string_value=SERVICES[service]['name'])
            ),
            KeyValue(
                key="status.code",
                value=AnyValue(string_value=status_code)
            ),
            KeyValue(
                key="duration.ms",
                value=AnyValue(double_value=duration / 1000000)
            )
        ]
    )
    
    return span, span_id_bytes, duration

def create_metric_data():
    """Create metrics data for each host"""
    resource_metrics_list = []
    current_time_nano = int(time.time() * 1e9)
    
    for host_id, host_info in HOSTS.items():
        # Generate random metrics
        cpu_usage = random.uniform(10, 90)
        memory_usage = random.uniform(20, 85)
        
        # Create metric data points
        cpu_datapoint = NumberDataPoint(
            start_time_unix_nano=current_time_nano,
            time_unix_nano=current_time_nano,
            as_double=cpu_usage,  # Use as_double instead of value
            attributes=[
                KeyValue(key="host.id", value=AnyValue(string_value=host_id))
            ]
        )
        
        memory_datapoint = NumberDataPoint(
            start_time_unix_nano=current_time_nano,
            time_unix_nano=current_time_nano,
            as_double=memory_usage * host_info['memory'] / 100,  # Convert percentage to actual MB
            attributes=[
                KeyValue(key="host.id", value=AnyValue(string_value=host_id))
            ]
        )
        
        # Create metrics
        cpu_metric = Metric(
            name="system.cpu.usage",
            description="CPU usage in percent",
            unit="%",
            gauge=Gauge(data_points=[cpu_datapoint])
        )
        
        memory_metric = Metric(
            name="system.memory.usage",
            description="Memory usage in MB",
            unit="MB",
            gauge=Gauge(data_points=[memory_datapoint])
        )
        
        # Create scope metrics
        scope = InstrumentationScope(
            name="com.example.monitoring",
            version="1.0"
        )
        
        scope_metrics = ScopeMetrics(
            scope=scope,
            metrics=[cpu_metric, memory_metric]
        )
        
        # Create resource metrics
        resource = Resource(
            attributes=[
                KeyValue(key="host.id", value=AnyValue(string_value=host_id)),
                KeyValue(key="host.cores", value=AnyValue(int_value=host_info['cpu_cores'])),
                KeyValue(key="host.memory", value=AnyValue(int_value=host_info['memory']))
            ]
        )
        
        resource_metrics = ResourceMetrics(
            resource=resource,
            scope_metrics=[scope_metrics]
        )
        
        resource_metrics_list.append(resource_metrics)
    
    return resource_metrics_list

def create_correlated_telemetry():
    trace_id = generate_w3c_trace_id()
    trace_id_bytes = bytes.fromhex(trace_id)
    current_time = int(time.time() * 1e9)  # current time in nanoseconds
    
    # Create a complex trace with multiple spans
    spans = []
    logs = []
    
    # Start with a root span from the order service
    root_span, root_span_id_bytes, root_duration = create_span(
        trace_id_bytes, 
        None, 
        'order', 
        'create_order',
        current_time
    )
    spans.append(root_span)
    
    # Add child spans from other services
    child_services = random.sample(['payment', 'inventory', 'shipping', 'notification'], 3)
    current_time += 1000000  # Add 1ms delay
    
    for service in child_services:
        operation = random.choice(SERVICES[service]['operations'])
        child_span, child_span_id_bytes, span_duration = create_span(
            trace_id_bytes,
            root_span_id_bytes,
            service,
            operation,
            current_time
        )
        spans.append(child_span)
        
        # Create correlated log entry
        log = LogRecord(
            time_unix_nano=current_time,
            severity_text="INFO",
            severity_number=9,
            body=AnyValue(string_value=f"Processing {operation}"),
            trace_id=trace_id_bytes,
            span_id=child_span_id_bytes,
            attributes=[
                KeyValue(
                    key="service.name",
                    value=AnyValue(string_value=SERVICES[service]['name'])
                ),
                KeyValue(
                    key="host.name",
                    value=AnyValue(string_value=random.choice(list(HOSTS.keys())))
                )
            ]
        )
        logs.append(log)
        current_time += span_duration + 1000000  # Add 1ms between spans

    # Create the resource and instrumentation scope
    resource = Resource(
        attributes=[
            KeyValue(
                key="service.namespace",
                value=AnyValue(string_value="production")
            ),
            KeyValue(
                key="service.version",
                value=AnyValue(string_value="1.0.0")
            )
        ]
    )

    instrumentation_scope = InstrumentationScope(
        name="com.example.service",
        version="1.0"
    )

    # Package everything up
    scope_spans = ScopeSpans(spans=spans)
    if hasattr(scope_spans, 'scope'):
        scope_spans.scope.CopyFrom(instrumentation_scope)
    elif hasattr(scope_spans, 'instrumentation_scope'):
        scope_spans.instrumentation_scope.CopyFrom(instrumentation_scope)

    resource_spans = ResourceSpans(
        resource=resource,
        scope_spans=[scope_spans]
    )

    trace_request = ExportTraceServiceRequest(
        resource_spans=[resource_spans]
    )

    # Create log request
    scope_logs = ScopeLogs(log_records=logs)
    if hasattr(scope_logs, 'scope'):
        scope_logs.scope.CopyFrom(instrumentation_scope)
    elif hasattr(scope_logs, 'instrumentation_scope'):
        scope_logs.instrumentation_scope.CopyFrom(instrumentation_scope)

    resource_logs = ResourceLogs(
        resource=resource,
        scope_logs=[scope_logs]
    )

    log_request = ExportLogsServiceRequest(
        resource_logs=[resource_logs]
    )

    return trace_request, log_request, trace_id

def send_telemetry(dt_url, api_token):
    trace_request, log_request, trace_id = create_correlated_telemetry()
    
    headers = {
        'Content-Type': 'application/x-protobuf',
        'Authorization': f'Api-Token {api_token}'
    }

    # Send traces
    trace_data = trace_request.SerializeToString()
    trace_response = requests.post(
        f'{dt_url}/api/v2/otlp/v1/traces',
        data=trace_data,
        headers=headers
    )
    
    # Send logs
    log_data = log_request.SerializeToString()
    log_response = requests.post(
        f'{dt_url}/api/v2/otlp/v1/logs',
        data=log_data,
        headers=headers
    )
    
    # Generate and send metrics
    resource_metrics_list = create_metric_data()
    metrics_request = ExportMetricsServiceRequest(
        resource_metrics=resource_metrics_list
    )
    metrics_data = metrics_request.SerializeToString()
    metrics_response = requests.post(
        f'{dt_url}/api/v2/otlp/v1/metrics',
        data=metrics_data,
        headers=headers
    )

    print("\nTrace ID (for correlation):", trace_id)
    print(f"\nTrace API Response: {trace_response.status_code}")
    print(f"Log API Response: {log_response.status_code}")
    print(f"Metrics API Response: {metrics_response.status_code}")

if __name__ == "__main__":
    # Replace these with your Dynatrace environment details
    DYNATRACE_URL = "https://<tenantId>.live.dynatrace.com" # Add the tenant id for your saas tenant or AG
    API_TOKEN = "ABC"  # Replace with your token, make sure you include the correct log, metric, and trace ingest scope
    
    send_telemetry(DYNATRACE_URL, API_TOKEN)
