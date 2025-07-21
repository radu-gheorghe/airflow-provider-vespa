from importlib.metadata import version

def get_provider_info():
    return {
        # --- basic metadata --------------------------------------------------
        "package-name": "airflow-provider-vespa",
        "name": "Vespa",
        "description": "Minimal provider that writes documents to a Vespa cluster",
        "versions": [version(__package__)],

        # --- integration(s) --------------------------------------------------
        # must reference the same integration-name you use below
        "integrations": [
            {
                "integration-name": "Vespa",
                "external-doc-url": "https://docs.vespa.ai",
                "tags": ["vespa", "search"],
            }
        ],

        # --- operators -------------------------------------------------------
        "operators": [
            {
                "integration-name": "Vespa",
                "python-modules": [
                    "airflow_provider_vespa.operators.vespa_ingest",
                ],
            }
        ],

        # --- hooks (optional but nice – lets UI map the connection) ---------
        "hooks": [
            {
                "integration-name": "Vespa",
                "python-modules": [
                    "airflow_provider_vespa.hooks.vespa",
                ],
            }
        ],

        # --- connection type (replaces deprecated hook-class-names) ---------
        "connection-types": [
            {
                "connection-type": "vespa",
                "hook-class-name": "airflow_provider_vespa.hooks.vespa.VespaHook",
            }
        ],
    }
