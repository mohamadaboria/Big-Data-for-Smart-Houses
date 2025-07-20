# 🏠 Smart House Data Platform

A comprehensive real-time IoT data platform that ingests smart home device telemetry, processes it through a medallion architecture (Bronze → Silver → Gold), and provides live analytics through an interactive dashboard.

## 🚀 Quick Start

### Prerequisites

1. **Docker Desktop** - [Download here](https://www.docker.com/products/docker-desktop)
2. **Python 3.11+** - [Download here](https://www.python.org/downloads/)
3. **Git** (optional) - For cloning the repository

### Windows Quick Start

1. **Download and extract** the project files to your desired directory
2. **Open Command Prompt** as Administrator and navigate to the project directory
3. **Run the startup script**:
   ```cmd
   start.bat || ./start
   ```

That's it! The script will:
- Check prerequisites
- Set up Python virtual environment
- Install dependencies
- Start Docker services (Kafka, Zookeeper, Kafka UI)
- Launch all pipeline components
- Open the dashboard

### Access Points

- **📊 Dashboard**: http://localhost:8501
- **🔧 Kafka UI**: http://localhost:8080

## 📋 What You'll See

The platform will automatically:

1. **Generate IoT Data**: Simulated smart home devices (thermostats, bulbs, plugs, cameras, sensors)
2. **Stream to Kafka**: Real-time telemetry data
3. **Process Data**: Through Bronze → Silver → Gold layers
4. **Display Dashboard**: Live charts, KPIs, and device health metrics

## 🏗️ Architecture Overview

```
IoT Devices → Kafka → Bronze Layer → Silver Layer → Gold Layer → Dashboard
    ↓           ↓         ↓            ↓             ↓           ↓
Simulated   Message   Raw Data    Cleaned &     Business    Live Web
Telemetry   Broker    (Parquet)   Enriched     Analytics      UI
                                  (Parquet)    (Parquet)
```

### Data Flow

1. **IoT Simulator** generates realistic telemetry data for 10 smart home devices
2. **Kafka** streams the data in real-time
3. **Bronze Layer** ingests raw data from Kafka and stores as Parquet files
4. **Silver Layer** validates, cleans, and enriches the data
5. **Gold Layer** creates business aggregations and analytics-ready datasets
6. **Dashboard** visualizes the processed data with live updates

## 📁 Project Structure

```
smart-house-platform/
├── 📄 start.bat              # Quick start script
├── 📄 stop.bat               # Stop all services
├── 📄 check_status.bat       # Check system status
├── 📄 docker-compose.yml     # Docker services configuration
├── 📄 requirements.txt       # Python dependencies
├── 📄 README.md              # This file
│
├── 📂 config/                # Configuration files
│   ├── device_catalog.csv    # Device metadata
│   ├── billing_data.csv      # Sample billing data
│   └── config.py             # Application settings
│
├── 📂 producer/              # Data generation
│   └── telemetry_sim.py      # IoT telemetry simulator
│
├── 📂 consumer/              # Data processing
│   ├── ingest_bronze.py      # Kafka to Bronze layer
│   ├── bronze_to_silver.py   # Bronze to Silver ETL
│   └── silver_to_gold.py     # Silver to Gold ETL
│
├── 📂 dashboard/             # Web interface
│   └── app.py                # Streamlit dashboard
│
├── 📂 scripts/               # Utility scripts
│   └── run_etl.py            # Automated ETL runner
│
└── 📂 data/                  # Data storage (auto-created)
    ├── bronze/               # Raw ingested data
    ├── silver/               # Cleaned and enriched data
    └── gold/                 # Business aggregations
```

## 🎯 Dashboard Features

### Key Performance Indicators (KPIs)
- **Total Energy**: Sum of energy consumption across all devices
- **Total Cost**: Estimated cost based on energy usage ($0.12/kWh)
- **Active Devices**: Count of unique devices reporting data
- **Average Health**: Mean device health score across all devices

### Charts and Visualizations
- **Energy Usage by Device Type**: Bar chart showing consumption by device category
- **Device Health vs Failure Risk**: Scatter plot with health scores and failure probabilities
- **Live Temperature Monitoring**: Real-time temperature trends by device
- **Live Power Usage**: Real-time power consumption trends
- **System Health Gauge**: Overall system health indicator

### Real-time Features
- **Auto-refresh**: Dashboard updates every 30 seconds
- **Live data**: Last 100 readings for temperature and power charts
- **Data status**: Sidebar showing file counts and last update times

## 🔧 Manual Operations

### Individual Component Control

If you need to run components individually:

```cmd
# Activate virtual environment first
call venv\Scripts\activate.bat

# Start individual components
python producer\telemetry_sim.py          # IoT data generator
python consumer\ingest_bronze.py          # Bronze layer consumer
python consumer\bronze_to_silver.py       # Bronze to Silver ETL
python consumer\silver_to_gold.py         # Silver to Gold ETL
python scripts\run_etl.py                 # Automated ETL runner
streamlit run dashboard\app.py            # Dashboard
```

### Docker Services

```cmd
# Start Docker services only
docker-compose up -d

# Stop Docker services
docker-compose down

# View logs
docker-compose logs -f
```

## 📊 Data Schema

### Bronze Layer (Raw Data)
```json
{
  "device_id": "device_001",
  "device_type": "thermostat",
  "user_id": "user_001",
  "timestamp": "2024-01-15T10:30:00Z",
  "temperature": 22.5,
  "power_usage": 1800.0,
  "energy_consumption_wh": 30.0,
  "status": "online",
  "alert": "none",
  "location": "living_room",
  "manufacturer": "Nest",
  "model": "Learning Thermostat"
}
```

### Silver Layer (Cleaned & Enriched)
- All Bronze fields plus:
- `quality_score`: Data quality indicator (0.0-1.0)
- `is_valid`: Boolean validation flag
- `is_late_event`: Late arrival detection
- `processing_delay_hours`: Processing latency
- `device_age_days`: Days since installation
- Derived metrics (rolling averages, efficiency, alert frequency)

### Gold Layer (Business Aggregations)
- **Daily Energy Consumption**: Device-level daily aggregations
- **Device Health Metrics**: Health scores, failure probabilities, reliability
- **Business Summary**: System-wide KPIs and metrics

## 🛠️ Troubleshooting

### Common Issues

#### 1. Docker Not Running
```
ERROR: Docker is not running
```
**Solution**: Start Docker Desktop and wait for it to fully initialize.

#### 2. Port Already in Use
```
ERROR: Port 9092 is already in use
```
**Solution**: Stop other Kafka instances or change ports in `docker-compose.yml`.

#### 3. Python Dependencies
```
ERROR: No module named 'kafka'
```
**Solution**: Ensure virtual environment is activated and run:
```cmd
pip install -r requirements.txt
```

#### 4. No Data in Dashboard
**Possible causes**:
- Producer not running → Check if IoT simulator is active
- Consumer not processing → Check Bronze consumer logs
- ETL not running → Check ETL processor status

**Solution**: Run `check_status.bat` to diagnose issues.

#### 5. Dashboard Not Loading
```
ERROR: streamlit: command not found
```
**Solution**: Activate virtual environment:
```cmd
call venv\Scripts\activate.bat
streamlit run dashboard\app.py
```

### Diagnostic Commands

```cmd
# Check overall status
check_status.bat

# Check Docker services
docker-compose ps

# Check Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check data files
dir /s data\*.parquet

# Check Python processes
tasklist | findstr python
```

### Log Locations

- **Docker logs**: `docker-compose logs [service_name]`
- **Python logs**: Console output in respective command windows
- **Kafka logs**: Available through Kafka UI at http://localhost:8080

## 🔄 ETL Processing Schedule

The automated ETL runner (`scripts\run_etl.py`) processes data on the following schedule:

- **Bronze to Silver**: Every 2 minutes (real-time processing)
- **Silver to Gold**: Every 5 minutes (business aggregations)
- **Full ETL**: Every hour (comprehensive processing)

## 📈 Performance Considerations

### Expected Throughput
- **Producer**: 10 messages/second (1 per device per second)
- **Consumer**: 300+ events/second processing capacity
- **Dashboard**: 30-second refresh interval for optimal performance

### Data Retention
- **Live charts**: Last 100 readings
- **File retention**: Configurable (default: unlimited)
- **Cache TTL**: 30 seconds for dashboard data

### Scaling Recommendations
- **Horizontal scaling**: Deploy multiple dashboard instances
- **Data partitioning**: Time-based partitioning implemented
- **Caching**: Redis integration for production deployments
- **Database**: PostgreSQL for production data storage

## 🔒 Security Notes

This is a **development/demo environment** with the following security considerations:

- **Local only**: Services bind to localhost
- **No authentication**: Kafka and dashboard have no auth
- **Plain text**: No TLS encryption
- **Development data**: Simulated data only

For production deployment, implement:
- TLS encryption for Kafka
- Authentication for dashboard
- Network security (VPN, firewalls)
- Data encryption at rest

## 🚀 Next Steps

### Immediate Enhancements
1. **Add more device types** in `config/device_catalog.csv`
2. **Customize alert thresholds** in `config/config.py`
3. **Modify dashboard layout** in `dashboard/app.py`
4. **Adjust ETL schedules** in `scripts/run_etl.py`

### Production Readiness
1. **Database integration** (PostgreSQL, InfluxDB)
2. **Authentication system** (OAuth, LDAP)
3. **Monitoring** (Prometheus, Grafana)
4. **Alerting** (Email, Slack notifications)
5. **Data backup** and disaster recovery

### Advanced Features
1. **Machine learning** models for predictive analytics
2. **Real-time alerting** system
3. **Mobile app** integration
4. **API endpoints** for external integrations
5. **Multi-tenant** support

## 📞 Support

For issues or questions:

1. **Check logs** in the respective component windows
2. **Run diagnostics** with `check_status.bat`
3. **Review troubleshooting** section above
4. **Check data flow** from producer → consumer → ETL → dashboard

## 📄 License

This project is provided as-is for educational and demonstration purposes.

---

**Happy monitoring! 🏠📊**

