#!/bin/bash
pip install -r ./requirements.txt
clear
jupyter nbconvert --to script pipeline.ipynb
pytest pipeline_test.py -s