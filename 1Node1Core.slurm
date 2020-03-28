#!/bin/bash
#SBATCH --time=00:10:00 
#SBATCH --nodes=1 
#SBATCH --ntasks=1 

printf "Run on 1 node and 1 core\n"
module load Python/3.7.3-spartan_gcc-8.1.0
mpiexec python3 script.py bigTwitter.json