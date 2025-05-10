#!/bin/bash

#SBATCH -p general
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --mem=300g
#SBATCH -t 1-00:00:00
#SBATCH --mail-type=all
#SBATCH --job-name=concat_data
#SBATCH --mail-user=kieranf@email.unc.edu

module purge
module load anaconda
export PYTHONWARNINGS="ignore"
conda activate /proj/characklab/projects/kieranf/flood_damage_index/fli-env-v1
python3.12 concatenate_matched_data.py
