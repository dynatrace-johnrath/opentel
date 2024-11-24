import logging
import yaml
from src.telemetry_generator import TelemetryGenerator
from src.models.config_models import Config

def load_config(config_path: str = 'config/telemetry_config.yaml') -> Config:
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
            if config_data is None:
                raise ValueError("Config file is empty")
            return Config(**config_data)
    except FileNotFoundError:
        logging.error(f"Configuration file '{config_path}' not found.")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML configuration: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error loading configuration: {str(e)}")
        raise

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        config = load_config()
        logging.info(f"Loaded configuration: URL={config.dynatrace_url}")
        
        generator = TelemetryGenerator(config)
        generator.run()
    except Exception as e:
        logging.error(f"Error during execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()