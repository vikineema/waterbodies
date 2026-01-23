#!/usr/bin/env bash
# ---------------- FHS micromamba shell ----------------
# Load the variables
set -a; source .env; set +a

echo "Creating temp directory at $TMPDIR"
mkdir -p $TMPDIR

ENV_NAME=$(yq -r '.name' $ENV_YAML)

if ! micromamba env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
    echo "Creating micromamba environment '$ENV_NAME'..."
    micromamba create -n "$ENV_NAME" -f "$ENV_YAML" -y
fi

# Activate environment
eval "$(micromamba shell hook --shell bash)"
micromamba activate "$ENV_NAME"

# Start interactive shell
exec bash