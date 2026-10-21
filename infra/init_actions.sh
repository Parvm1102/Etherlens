#!/usr/bin/env bash
set -euxo pipefail

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "$ROLE" == "Master" ]]; then
    # tmux is not part of the Dataproc 2.2 image; start_poller.sh needs it
    apt-get install -y -q tmux || (apt-get update -q && apt-get install -y -q tmux)
    /opt/conda/default/bin/python -m venv /opt/poller-venv
    /opt/poller-venv/bin/pip install --quiet "web3>=7,<8" "google-cloud-storage>=2.14"
fi
