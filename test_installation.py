"""
Installation Test Script
Validates that all components are properly installed and configured
Compatible with Python 3.11+
"""
import sys
import subprocess
import importlib
from pathlib import Path
from typing import List, Tuple, Dict, Any

def test_python_version() -> bool:
    """Test Python version"""
    print("Testing Python version...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 11:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} - OK")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} - Requires Python 3.11+")
        return False

def test_required_packages() -> bool:
    """Test required Python packages"""
    print("\nTesting required packages...")
    required_packages = [
        'kafka',
        'pandas',
        'streamlit',
        'plotly',
        'numpy',
        'pyarrow',
        'schedule',
        'watchdog',
        'psutil'
    ]
    
    all_ok = True
    for package in required_packages:
        try:
            importlib.import_module(package)
            print(f"✅ {package} - OK")
        except ImportError:
            print(f"❌ {package} - Missing")
            all_ok = False
    
    return all_ok

def test_docker() -> bool:
    """Test Docker installation"""
    print("\nTesting Docker...")
    try:
        result = subprocess.run(['docker', '--version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print(f"✅ Docker - {result.stdout.strip()}")
            return True
        else:
            print("❌ Docker - Not working properly")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("❌ Docker - Not installed or not in PATH")
        return False

def test_docker_compose() -> bool:
    """Test Docker Compose"""
    print("\nTesting Docker Compose...")
    try:
        result = subprocess.run(['docker-compose', '--version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print(f"✅ Docker Compose - {result.stdout.strip()}")
            return True
        else:
            print("❌ Docker Compose - Not working properly")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("❌ Docker Compose - Not installed or not in PATH")
        return False

def test_project_structure() -> bool:
    """Test project directory structure"""
    print("\nTesting project structure...")
    
    required_files = [
        'docker-compose.yml',
        'requirements.txt',
        'start.bat',
        'stop.bat',
        'config/config.py',
        'config/device_catalog.csv',
        'producer/telemetry_sim.py',
        'consumer/ingest_bronze.py',
        'consumer/bronze_to_silver.py',
        'consumer/silver_to_gold.py',
        'dashboard/app.py',
        'scripts/run_etl.py'
    ]
    
    all_ok = True
    for file_path in required_files:
        if Path(file_path).exists():
            print(f"✅ {file_path} - OK")
        else:
            print(f"❌ {file_path} - Missing")
            all_ok = False
    
    return all_ok

def test_config_files() -> bool:
    """Test configuration files"""
    print("\nTesting configuration files...")
    
    try:
        # Test device catalog
        import pandas as pd
        devices_df = pd.read_csv('config/device_catalog.csv')
        print(f"✅ Device catalog - {len(devices_df)} devices loaded")
        
        # Test config import
        sys.path.append('.')
        from config.config import KAFKA_BOOTSTRAP_SERVERS, DATA_DIR
        print(f"✅ Configuration - Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration files - Error: {e}")
        return False

def test_data_directories() -> bool:
    """Test data directory structure"""
    print("\nTesting data directories...")
    
    data_dirs = [
        'data',
        'data/bronze',
        'data/silver', 
        'data/gold',
        'data/bronze/telemetry',
        'data/silver/energy_usage',
        'data/gold/daily_energy_consumption',
        'data/gold/device_health_metrics',
        'data/gold/daily_business_summary'
    ]
    
    all_ok = True
    for dir_path in data_dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        if Path(dir_path).exists():
            print(f"✅ {dir_path}/ - OK")
        else:
            print(f"❌ {dir_path}/ - Failed to create")
            all_ok = False
    
    return all_ok

def test_imports() -> bool:
    """Test Python script imports"""
    print("\nTesting script imports...")
    
    scripts: List[Tuple[str, str]] = [
        ('producer.telemetry_sim', 'IoTTelemetrySimulator'),
        ('consumer.ingest_bronze', 'BronzeLayerConsumer'),
        ('consumer.bronze_to_silver', 'BronzeToSilverETL'),
        ('consumer.silver_to_gold', 'SilverToGoldETL')
    ]
    
    all_ok = True
    for module_name, class_name in scripts:
        try:
            module = importlib.import_module(module_name)
            getattr(module, class_name)
            print(f"✅ {module_name}.{class_name} - OK")
        except Exception as e:
            print(f"❌ {module_name}.{class_name} - Error: {e}")
            all_ok = False
    
    return all_ok

def main() -> bool:
    """Run all tests"""
    print("🏠 Smart House Data Platform - Installation Test")
    print("=" * 50)
    
    tests = [
        test_python_version,
        test_required_packages,
        test_docker,
        test_docker_compose,
        test_project_structure,
        test_config_files,
        test_data_directories,
        test_imports
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    print("📋 Test Summary")
    print("=" * 50)
    
    passed = sum(results)
    total = len(results)
    
    if passed == total:
        print(f"🎉 All tests passed! ({passed}/{total})")
        print("\n✅ Installation is ready!")
        print("Run 'start.bat' to launch the platform.")
    else:
        print(f"⚠️  {passed}/{total} tests passed")
        print(f"❌ {total - passed} tests failed")
        print("\n🔧 Please fix the issues above before running the platform.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

