#!/bin/bash
#SBATCH --time=00:10:00 
#SBATCH --nodes=1 
#SBATCH --ntasks=8 

printf "Run on 1 node and 8 cores\n"
module load Python/3.4.3-goolf-2015a
mpiexec python3 script.py bigTwitter.json