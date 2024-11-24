import yaml
import random
import os
from typing import List, Dict, Any
from opentelemetry.proto.trace.v1.trace_pb2 import Span
from opentelemetry.proto.common.v1.common_pb2 import KeyValue, AnyValue
from .models.config_models import AWSService, Database

class DistributedTraceManager:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
            
        self.aws_services = self._load_aws_services()
        self.databases = self._load_databases()
        
    def _load_aws_services(self) -> Dict[str, List[AWSService]]:
        services = {}
        for service_type, items in self.config['aws_services'].items():
            services[service_type] = [
                AWSService(
                    name=item['name'],
                    service_type=item['service_type'],
                    region=item['region']
                ) for item in items
            ]
        return services
        
    def _load_databases(self) -> List[Database]:
        return [
            Database(
                name=db['name'],
                type=db['type'],
                operation_times=db['operation_times'],
                error_rate=db['error_rate']
            ) for db in self.config['databases']
        ]
        
    def create_distributed_trace(self, trace_id_bytes: bytes, parent_span_id_bytes: bytes,
                               current_time: int, conversation: Dict[str, Any]) -> List[Span]:
        spans = []
        
        # API Gateway entry
        api_gateway = random.choice(self.aws_services['api_gateways'])
        gateway_span = self._create_aws_span(
            trace_id_bytes, parent_span_id_bytes,
            api_gateway, 'process_request',
            current_time, 50, 150
        )
        spans.append(gateway_span)
        current_time += random.randint(50000000, 150000000)  # 50-150ms
        
        # Lambda for preprocessing
        preprocess_lambda = next(l for l in self.aws_services['lambda_functions'] 
                               if l.name == 'prompt-preprocessor')
        preprocess_span = self._create_aws_span(
            trace_id_bytes, gateway_span.span_id,
            preprocess_lambda, 'preprocess_prompt',
            current_time, 100, 300
        )
        spans.append(preprocess_span)
        current_time += random.randint(100000000, 300000000)
        
        # Database calls for context
        context_db = next(db for db in self.databases if db.name == 'context-store')
        db_span = self._create_database_span(
            trace_id_bytes, preprocess_span.span_id,
            context_db, 'fetch_context',
            current_time, conversation
        )
        spans.append(db_span)
        current_time += random.randint(
            context_db.operation_times['read'][0] * 1000000,
            context_db.operation_times['read'][1] * 1000000
        )
        
        # SQS queue for prompt processing
        prompt_queue = next(q for q in self.aws_services['sqs_queues'] 
                          if q.name == 'prompt-processing-queue')
        queue_span = self._create_aws_span(
            trace_id_bytes, preprocess_span.span_id,
            prompt_queue, 'enqueue_prompt',
            current_time, 20, 50
        )
        spans.append(queue_span)
        current_time += random.randint(20000000, 50000000)
        
        # Response processing lambda
        response_lambda = next(l for l in self.aws_services['lambda_functions'] 
                             if l.name == 'response-postprocessor')
        response_span = self._create_aws_span(
            trace_id_bytes, queue_span.span_id,
            response_lambda, 'process_response',
            current_time, 200, 500,
            attributes=[
                KeyValue(key="ai.response", value=AnyValue(string_value=conversation["response"])),
                KeyValue(key="ai.tokens", value=AnyValue(int_value=conversation["tokens"])),
                KeyValue(key="ai.model", value=AnyValue(string_value=conversation["model"]))
            ]
        )
        spans.append(response_span)
        
        return spans

    def _create_aws_span(self, trace_id_bytes: bytes, parent_span_id_bytes: bytes,
                        service: AWSService, operation: str, start_time: int,
                        min_duration: int, max_duration: int,
                        attributes: List[KeyValue] = None) -> Span:
        span_id = os.urandom(8)
        duration = random.randint(min_duration * 1000000, max_duration * 1000000)
        
        base_attributes = [
            KeyValue(key="aws.service", value=AnyValue(string_value=service.service_type)),
            KeyValue(key="aws.region", value=AnyValue(string_value=service.region)),
            KeyValue(key="aws.operation", value=AnyValue(string_value=operation)),
            KeyValue(key="service.name", value=AnyValue(string_value=service.name))
        ]
        
        if attributes:
            base_attributes.extend(attributes)
        
        return Span(
            trace_id=trace_id_bytes,
            span_id=span_id,
            parent_span_id=parent_span_id_bytes,
            name=f"{service.name}.{operation}",
            kind=Span.SpanKind.SPAN_KIND_SERVER,
            start_time_unix_nano=start_time,
            end_time_unix_nano=start_time + duration,
            attributes=base_attributes
        )

    def _create_database_span(self, trace_id_bytes: bytes, parent_span_id_bytes: bytes,
                            database: Database, operation: str, start_time: int,
                            conversation: Dict[str, Any]) -> Span:
        span_id = os.urandom(8)
        duration = random.randint(
            database.operation_times['read'][0] * 1000000,
            database.operation_times['read'][1] * 1000000
        )
        
        attributes = [
            KeyValue(key="db.system", value=AnyValue(string_value=database.type)),
            KeyValue(key="db.name", value=AnyValue(string_value=database.name)),
            KeyValue(key="db.operation", value=AnyValue(string_value=operation)),
            KeyValue(key="db.statement", value=AnyValue(string_value=f"SELECT context FROM user_history WHERE prompt_type = '{conversation['prompt'][:20]}'")),
            KeyValue(key="service.name", value=AnyValue(string_value=database.name))
        ]
        
        return Span(
            trace_id=trace_id_bytes,
            span_id=span_id,
            parent_span_id=parent_span_id_bytes,
            name=f"{database.name}.{operation}",
            kind=Span.SpanKind.SPAN_KIND_CLIENT,
            start_time_unix_nano=start_time,
            end_time_unix_nano=start_time + duration,
            attributes=attributes
        )