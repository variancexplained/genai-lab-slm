project_root/
│
├── data_prep/
│   ├── application/
│   ├── domain/
│   ├── ui/
│   └── config/
│
├── eda/
│   ├── application/
│   ├── domain/
│   ├── ui/
│   └── config/
│
├── sentiment_analysis/
│   ├── application/
│   ├── domain/
│   ├── ui/
│   └── config/
│
├── topic_modeling/
│   ├── application/
│   ├── domain/
│   ├── ui/
│   └── config/
│
└── shared/
    ├── application/
    │   ├── task.py         <-- Shared Task class
    │   └── other_app_module.py
    │
    ├── domain/
    │   ├── common_model.py
    │   └── other_domain_module.py
    │
    ├── ui/
    │   ├── shared_visualization.py
    │   └── other_ui_module.py
    │
    ├── config/
    │   ├── base_config.py
    │   ├── global_config.py
    │   ├── dev.yaml
    │   ├── prod.yaml
    │   └── test.yaml
    │
    └── infrastructure/
        ├── io.py
        └── config_loader.py
