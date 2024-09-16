project_root/
├── core/
│   ├── __init__.py
│   ├── interfaces/
│   │   ├── i_pipeline.py
│   │   ├── i_task.py
│   │   ├── i_io.py
│   │   └── i_builder.py
│   └── models/
│       ├── dataset.py
│       └── result.py
├── features/
│   ├── data_prep/
│   │   ├── __init__.py
│   │   ├── pipeline.py
│   │   ├── tasks/
│   │   │   ├── __init__.py
│   │   │   ├── reader.py
│   │   │   ├── writer.py
│   │   └── builder.py
│   ├── eda/
│   │   ├── __init__.py
│   │   ├── pipeline.py
│   │   ├── tasks/
│   │   │   ├── __init__.py
│   │   │   └── some_eda_task.py
│   │   └── builder.py
│   └── modeling/
│       ├── __init__.py
│       ├── pipeline.py
│       ├── tasks/
│       │   ├── __init__.py
│       │   └── some_modeling_task.py
│       └── builder.py
└── shared/
    ├── __init__.py
    ├── config/
    │   ├── base_config.py
    │   ├── dev.yaml
    │   ├── prod.yaml
    │   └── test.yaml
    ├── dependency_container.py
    ├── frameworks/
    │   └── spark/
    │       ├── spark_utils.py
    │       └── spark_integration.py
    ├── instrumentation/
    │   ├── performance_profiler.py
    │   └── metrics.py
    ├── logging/
    │   ├── logger.py
    │   └── log_config.py
    ├── persist/
    │   ├── io.py
    │   └── database.py
    ├── recovery/
    │   ├── backup.py
    │   └── restore.py
    ├── setup/
    │   ├── database_setup.py
    │   └── environment_setup.py
    └── utils/
        ├── data_utils.py
        └── general_utils.py
