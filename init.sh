#!/bin/bash

VENV_DIR="venv"

if [ ! -d ./${VENV_DIR} ]
then
  python3 -m venv ${VENV_DIR}
  source ./${VENV_DIR}/bin/activate
  pip install --upgrade pip
  pip install -r requirements.txt
else
  source ./${VENV_DIR}/bin/activate
fi
