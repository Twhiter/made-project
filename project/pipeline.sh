#!/bin/bash
pip install -r ./requirements.txt
clear
jupyter nbconvert --to script pipeline.ipynb
python3 pipeline.py
