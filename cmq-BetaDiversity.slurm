#! /bin/bash -l
#SBATCH --job-name=Beta_diversity
#SBATCH --ntasks=1 --cpus-per-task=10 --mem-per-cpu=6000mb
#SBATCH --time=48:35:00
#SBATCH -A lp_climateplants

source $VSC_HOME/.bashrc

cd $VSC_DATA/MakeTiles_CutFutBinaryStacks/
export OMP_NUM_THREADS=1

source activate VoCC_R_new

Rscript targets_run_cmq.R
