#!/bin/bash
#SBATCH --partition=physical
#SBATCH --time=00:10:00 
#SBATCH --nodes=2 
#SBATCH --ntasks-per-node=4

printf "Run on 2 node and 8 cores\n"
module load Python/3.4.3-goolf-2015a
mpiexec python3 script.py bigTwitter.json