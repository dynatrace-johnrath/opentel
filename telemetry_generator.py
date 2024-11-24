import yaml
import time
import random
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
import signal
import sys
import requests
from typing import Tuple, Any, List

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

from .distributed_trace_manager import DistributedTraceManager
from .models.config_models import Config

def generate_w3c_trace_id() -> str:
    """Generate a valid W3C trace ID (16 bytes, 32 hex characters)"""
    random_bytes = os.urandom(16)
    trace_id = random_bytes.hex()
    logging.debug(f"Generated W3C trace ID: {trace_id}")
    return trace_id

def generate_w3c_span_id() -> str:
    """Generate a valid W3C span ID (8 bytes, 16 hex characters)"""
    random_bytes = os.urandom(8)
    return random_bytes.hex()

class TelemetryGenerator:
    def __init__(self, config: Config):
        self.config = config
        self.trace_queue = Queue()
        self.log_queue = Queue()
        self.metric_queue = Queue()
        self.running = False
        self.start_time = None
        self.telemetry_counts = {
            'traces': 0,
            'logs': 0,
            'metrics': 0
        }
        
        # Load conversations and distributed services
        with open('config/ai_conversations.yaml', 'r') as f:
            self.conversations = yaml.safe_load(f)['conversations']
        
        self.distributed_trace_manager = DistributedTraceManager('config/distributed_services.yaml')

    def generate_metrics(self) -> ExportMetricsServiceRequest:
        """Generate AI-specific metrics"""
        resource_metrics_list = []
        current_time_nano = int(time.time() * 1e9)
        
        # AI-specific metrics
        ai_metrics = {
            'ai.inference.latency': random.uniform(100, 2000),
            'ai.tokens.total': random.randint(1000, 10000),
            'ai.requests.total': random.randint(100, 1000),
            'ai.error.rate': random.uniform(0, 0.1),
            'ai.user.satisfaction': random.uniform(0.7, 1.0),
            'ai.prompt.tokens': random.randint(500, 5000),
            'ai.completion.tokens': random.randint(500, 5000),
            'ai.context.length': random.randint(1000, 8000)
        }
        
        for metric_name, value in ai_metrics.items():
            datapoint = NumberDataPoint(
                start_time_unix_nano=current_time_nano,
                time_unix_nano=current_time_nano,
                as_double=value,
                attributes=[
                    KeyValue(key="service.name", value=AnyValue(string_value="ai-service")),
                    KeyValue(key="deployment.environment", value=AnyValue(string_value="production"))
                ]
            )
            
            metric = Metric(
                name=metric_name,
                description=f"AI metric: {metric_name}",
                unit="1",
                gauge=Gauge(data_points=[datapoint])
            )
            
            scope = InstrumentationScope(
                name="com.example.ai.monitoring",
                version="2.0"
            )
            
            scope_metrics = ScopeMetrics(
                scope=scope,
                metrics=[metric]
            )
            
            resource = Resource(
                attributes=[
                    KeyValue(key="service.name", value=AnyValue(string_value="ai-service")),
                    KeyValue(key="environment", value=AnyValue(string_value="production"))
                ]
            )
            
            resource_metrics = ResourceMetrics(
                resource=resource,
                scope_metrics=[scope_metrics]
            )
            
            resource_metrics_list.append(resource_metrics)
        
        return ExportMetricsServiceRequest(resource_metrics=resource_metrics_list)

    def create_span(self, trace_id_bytes: bytes, parent_span_id_bytes: bytes, 
                   service_name: str, operation: str, start_time_nano: int,
                   attributes: List[KeyValue] = None) -> Tuple[Span, bytes, int]:
        """Create a single span with the given parameters"""
        span_id = generate_w3c_span_id()
        span_id_bytes = bytes.fromhex(span_id)
        
        # Generate random duration between 50ms and 500ms
        duration = random.randint(50000000, 500000000)  # 50-500ms in nanoseconds
        
        # Base attributes for the span
        base_attributes = [
            KeyValue(key="service.name", value=AnyValue(string_value=service_name)),
            KeyValue(key="operation", value=AnyValue(string_value=operation))
        ]
        
        if attributes:
            base_attributes.extend(attributes)
        
        span = Span(
            trace_id=trace_id_bytes,
            span_id=span_id_bytes,
            parent_span_id=parent_span_id_bytes if parent_span_id_bytes else None,
            name=operation,
            kind=Span.SpanKind.SPAN_KIND_SERVER,
            start_time_unix_nano=start_time_nano,
            end_time_unix_nano=start_time_nano + duration,
            attributes=base_attributes
        )
        
        return span, span_id_bytes, duration

    def generate_ai_trace(self) -> Tuple[ExportTraceServiceRequest, ExportLogsServiceRequest, str]:
        """Generate a single trace with all its spans and AI metadata"""
        trace_id = generate_w3c_trace_id()
        trace_id_bytes = bytes.fromhex(trace_id)
        current_time = int(time.time() * 1e9)
        
        conversation = random.choice(self.conversations)
        spans = []
        logs = []
        
        # Create root span from frontend service
        root_span, root_span_id_bytes, root_duration = self.create_span(
            trace_id_bytes,
            None,
            'ai-frontend-service',
            'process_user_input',
            current_time,
            attributes=[
                KeyValue(key="ai.prompt", value=AnyValue(string_value=conversation["prompt"])),
                KeyValue(key="ai.model", value=AnyValue(string_value=conversation["model"])),
                KeyValue(key="ai.tokens", value=AnyValue(int_value=conversation["tokens"]))
            ]
        )
        
        spans.append(root_span)
        current_time += root_duration
        
        # Generate distributed trace spans
        distributed_spans = self.distributed_trace_manager.create_distributed_trace(
            trace_id_bytes,
            root_span_id_bytes,
            current_time,
            conversation
        )
        
        spans.extend(distributed_spans)
        
        # Create correlated log entries
        for span in spans:
            log = LogRecord(
                time_unix_nano=span.start_time_unix_nano,
                severity_text="INFO",
                severity_number=9,
                body=AnyValue(string_value=f"Processing span: {span.name}"),
                trace_id=trace_id_bytes,
                span_id=span.span_id,
                attributes=[attr for attr in span.attributes]
            )
            logs.append(log)
        
        return self.package_telemetry(spans, logs, trace_id)

    def package_telemetry(self, spans: List[Span], logs: List[LogRecord], trace_id: str) -> Tuple[ExportTraceServiceRequest, ExportLogsServiceRequest, str]:
        """Package spans and logs into OTLP requests"""
        resource = Resource(
            attributes=[
                KeyValue(key="service.namespace", value=AnyValue(string_value="ai-production")),
                KeyValue(key="service.version", value=AnyValue(string_value="2.0.0"))
            ]
        )

        instrumentation_scope = InstrumentationScope(
            name="com.example.ai.service",
            version="2.0"
        )

        # Package traces
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

        # Package logs
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

    def process_queue(self, queue: Queue, batch_type: str, interval: int) -> None:
        """Process a queue of telemetry data and send in batches"""
        batch = []
        batch_trace_ids = []
        last_send_time = time.time()
        
        while self.running or not queue.empty():
            try:
                item = queue.get(timeout=1)
                
                if batch_type == 'traces':
                    request, trace_id = item
                    batch.append(request)
                    batch_trace_ids.append(trace_id)
                else:
                    batch.append(item)
                
                current_time = time.time()
                if current_time - last_send_time >= interval and batch:
                    if batch_type == 'traces':
                        batch_size = len(batch)
                        logging.info(f"Sending {batch_type} batch with {batch_size} items")
                        logging.info(f"Trace IDs in batch: {', '.join(batch_trace_ids[-3:])}")
                        send_success = self.send_batch(batch_type, batch[-1])
                        if send_success:
                            logging.info(f"Successfully sent {batch_size} {batch_type}")
                        batch_trace_ids = []
                    else:
                        batch_size = len(batch)
                        logging.info(f"Sending {batch_type} batch with {batch_size} items")
                        send_success = self.send_batch(batch_type, batch[-1])
                        if send_success:
                            logging.info(f"Successfully sent {batch_size} {batch_type}")
                    
                    batch = []
                    last_send_time = current_time
                    
            except Empty:
                current_time = time.time()
                if batch and current_time - last_send_time >= interval:
                    if batch_type == 'traces':
                        batch_size = len(batch)
                        logging.info(f"Sending final {batch_type} batch with {batch_size} items")
                        logging.info(f"Trace IDs in batch: {', '.join(batch_trace_ids[-3:])}")
                        self.send_batch(batch_type, batch[-1])
                    else:
                        batch_size = len(batch)
                        logging.info(f"Sending final {batch_type} batch with {batch_size} items")
                        self.send_batch(batch_type, batch[-1])
                    batch = []
                    batch_trace_ids = []
                    last_send_time = current_time
                continue

    def send_batch(self, batch_type: str, data: Any) -> bool:
        """Send a batch of telemetry data to Dynatrace"""
        headers = {
            'Content-Type': 'application/x-protobuf',
            'Authorization': f'Api-Token {self.config.api_token}'
        }
        
        endpoint_map = {
            'traces': '/api/v2/otlp/v1/traces',
            'logs': '/api/v2/otlp/v1/logs',
            'metrics': '/api/v2/otlp/v1/metrics'
        }
        
        full_url = f'{self.config.dynatrace_url}{endpoint_map[batch_type]}'
        
        try:
            logging.info(f"Sending {batch_type} to URL: {full_url}")
            logging.info(f"Headers: {headers}")
            
            serialized_data = data.SerializeToString()
            logging.info(f"Serialized data size: {len(serialized_data)} bytes")
            
            start_time = time.time()
            response = requests.post(
                full_url,
                data=serialized_data,
                headers=headers,
                timeout=10
            )
            duration = (time.time() - start_time) * 1000
            
            logging.info(f"{batch_type.capitalize()} batch sent. "
                        f"Status: {response.status_code}, "
                        f"Duration: {duration:.2f}ms, "
                        f"Response Length: {len(response.content)}")
            
            if response.status_code != 200:
                logging.error(f"Non-200 response for {batch_type}: {response.status_code}")
                logging.error(f"Response content: {response.text}")
                logging.error(f"Response headers: {response.headers}")
                return False
            else:
                logging.info(f"Successfully sent {batch_type} batch")
                return True
                
        except Exception as e:
            logging.error(f"Error sending {batch_type} batch: {str(e)}")
            logging.error("Full exception details:", exc_info=True)
            return False
        
    def generate_telemetry(self) -> None:
        """Generate telemetry data continuously"""
        while self.running:
            current_time = time.time()
            elapsed_minutes = (current_time - self.start_time) / 60
            
            if elapsed_minutes >= self.config.duration_minutes:
                logging.info(f"Reached target duration of {self.config.duration_minutes} minutes. Stopping generation.")
                self.running = False
                break
            
            # Generate and queue telemetry
            trace_request, log_request, trace_id = self.generate_ai_trace()
            metric_request = self.generate_metrics()
            
            # Queue the generated data
            self.trace_queue.put((trace_request, trace_id))
            self.log_queue.put(log_request)
            self.metric_queue.put(metric_request)
            
            self.telemetry_counts['traces'] += 1
            self.telemetry_counts['logs'] += 1
            self.telemetry_counts['metrics'] += 1
            
            # Log progress every 5th trace
            if self.telemetry_counts['traces'] % 5 == 0:
                logging.info(f"Sample Trace ID for lookup: {trace_id}")
                
            # Log overall progress every 10 items
            if self.telemetry_counts['traces'] % 10 == 0:
                logging.info(f"Generated: Traces={self.telemetry_counts['traces']}, "
                           f"Logs={self.telemetry_counts['logs']}, "
                           f"Metrics={self.telemetry_counts['metrics']}")
                logging.info(f"Elapsed time: {elapsed_minutes:.2f} minutes")
            
            time.sleep(1)  # Prevent overwhelming the system

    def run(self) -> None:
        """Main execution method"""
        self.running = True
        self.start_time = time.time()
        
        logging.info(f"Starting telemetry generation for {self.config.duration_minutes} minutes")
        logging.info(f"Intervals - Traces: {self.config.trace_interval}s, "
                    f"Logs: {self.config.log_interval}s, "
                    f"Metrics: {self.config.metric_interval}s")
        
        # Start telemetry generation thread
        generator_thread = threading.Thread(
            target=self.generate_telemetry,
            name="TelemetryGenerator"
        )
        generator_thread.start()
        
        # Start processing threads with descriptive names
        trace_thread = threading.Thread(
            target=self.process_queue,
            args=(self.trace_queue, 'traces', self.config.trace_interval),
            name="TraceProcessor"
        )
        log_thread = threading.Thread(
            target=self.process_queue,
            args=(self.log_queue, 'logs', self.config.log_interval),
            name="LogProcessor"
        )
        metric_thread = threading.Thread(
            target=self.process_queue,
            args=(self.metric_queue, 'metrics', self.config.metric_interval),
            name="MetricProcessor"
        )
        
        trace_thread.start()
        log_thread.start()
        metric_thread.start()
        
        # Wait for generator to finish
        generator_thread.join()
        
        # Wait a bit longer for queues to be processed
        time.sleep(max(self.config.trace_interval, self.config.log_interval, self.config.metric_interval))
        
        # Signal threads to stop
        self.running = False
        
        # Wait for processing threads to complete
        trace_thread.join()
        log_thread.join()
        metric_thread.join()
        
        end_time = time.time()
        duration = (end_time - self.start_time) / 60
        
        logging.info("Telemetry generation completed")
        logging.info(f"Total duration: {duration:.2f} minutes")
        logging.info(f"Final counts - Traces: {self.telemetry_counts['traces']}, "
                    f"Logs: {self.telemetry_counts['logs']}, "
                    f"Metrics: {self.telemetry_counts['metrics']}")