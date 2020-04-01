#!/bin/bash

#SBATCH --partition=physical
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --cpus-per-task=1
#SBATCH --output=physical_1n16c.out
#SBATCH --mail-user=xinyaon@student.unimelb.edu.au
#SBATCH --mail-type=End
module load Python/3.6.1-intel-2017.u2
mpirun -n 16 python testCluster.py
