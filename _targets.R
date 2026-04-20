# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
library(tarchetypes)
library(clustermq)

## Running on HPC with Slurm:
# Settings for clustermq
options(
  clustermq.scheduler = "slurm",
  clustermq.template = "./cmq.tmpl" # if using your own template
)

# Running locally on Windows
# options(clustermq.scheduler = "multiprocess")

# Set target options:
tar_option_set(
  resources = tar_resources(
    clustermq = tar_resources_clustermq(template = list(
      job_name = "Beta_diversity",
      per_cpu_mem = "3000mb", #"3470mb"(wice thin node), #"21000mb" (genius bigmem， hugemem)"5100mb"
      n_tasks = 1,
      per_task_cpus = 72,
      walltime = "48:00:00"
    ))
  )
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source()

# ---------------------------------------------------------
# Pipeline
# ---------------------------------------------------------
list(
  # -----------------------
  # Directories
  # -----------------------
  tar_target(
    cur_tile_dir,
    "/lustre1/scratch/348/vsc34871/output/Binary_CurrentActual_RedList_tiles/"
  ),

  tar_target(
    fut_tile_dir,
    "/lustre1/scratch/348/vsc34871/output/Binary_FutureReachable_RedList_tiles/"
  ),

  tar_target(
    out_dir,
    "/lustre1/scratch/348/vsc34871/output/RedList_tile_output_Beta/"
  ),

  # # -----------------------
  # # Set the single tile you want to process
  # # -----------------------
  # tar_target(
  #   target_tile_ids,
  #   normalize_tile_id(c(14, 17))
  # ),
  # -----------------------
  # Match all current/future tile pairs
  # -----------------------
  tar_target(
    tile_pairs,
    {
      tp <- match_tile_files(cur_tile_dir, fut_tile_dir)

      if (nrow(tp) == 0) {
        stop("No matched tile pairs found.")
      }

      tp
    },
    format = "rds"
  ),

  #  # -----------------------
  # # Keep only the requested tile
  # # -----------------------
  # tar_target(
  #   tile_pairs,
  #   {
  #     tp <- tile_pairs_all[tile_pairs_all$tile_id %in% target_tile_ids, , drop = FALSE]

  #     missing_ids <- setdiff(target_tile_ids, tp$tile_id)
  #     if (length(missing_ids) > 0) {
  #       stop("Requested tile_id(s) not found: ", paste(missing_ids, collapse = ", "))
  #     }

  #     tp[order(match(tp$tile_id, target_tile_ids)), , drop = FALSE]
  #   },
  #   format = "rds"
  # ),

  tar_target(
    tile_output,
    process_one_tile_beta(
      cur_tile = tile_pairs$cur_tile,
      fut_tile = tile_pairs$fut_tile,
      out_dir  = out_dir,
      overwrite = TRUE,
      tile_id = tile_pairs$tile_id
    ),
    pattern = map(tile_pairs),
    iteration = "list",
    format = "rds"
  )

#   # -----------------------
#   # Collect ALL finished tile outputs from disk
#   # -----------------------
#   tar_target(
#     rich_cur_tiles,
#     list_metric_tiles(out_dir, "^tile_\\d+_richness_current\\.tif$"),
#     format = "file"
#   ),

#   tar_target(
#     rich_fut_tiles,
#     list_metric_tiles(out_dir, "^tile_\\d+_richness_future\\.tif$"),
#     format = "file"
#   ),

#   tar_target(
#     rich_change_tiles,
#     list_metric_tiles(out_dir, "^tile_\\d+_richness_change\\.tif$"),
#     format = "file"
#   ),

#   tar_target(
#     rich_ratio_tiles,
#     list_metric_tiles(out_dir, "^tile_\\d+_richness_ratio\\.tif$"),
#     format = "file"
#   ),

#   tar_target(
#     abc_tiles,
#     list_metric_tiles(out_dir, "^tile_\\d+_abc\\.tif$"),
#     format = "file"
#   ),

#   tar_target(
#     beta_tiles,
#     list_metric_tiles(out_dir, "^tile_\\d+_beta\\.tif$"),
#     format = "file"
#   ),

# # -----------------------
#   # Merge Europe-wide rasters
#   # -----------------------
#   tar_target(
#   merge_dir,
#   "/lustre1/scratch/348/vsc34871/output/Merge_beta_EU/"
# ),
#   tar_target(
#     final_rich_cur,
#     merge_tile_outputs(
#       files = rich_cur_tiles,
#       out_file = file.path(merge_dir, "EU_richness_current.tif"),
#       wopt = wopt_int2u()
#     ),
#     format = "file"
#   ),

#   tar_target(
#     final_rich_fut,
#     merge_tile_outputs(
#       files = rich_fut_tiles,
#       out_file = file.path(merge_dir, "EU_richness_future.tif"),
#       wopt = wopt_int2u()
#     ),
#     format = "file"
#   ),

#   tar_target(
#     final_rich_change,
#     merge_tile_outputs(
#       files = rich_change_tiles,
#       out_file = file.path(merge_dir, "EU_richness_change_future_minus_current.tif"),
#       wopt = wopt_int2s()
#     ),
#     format = "file"
#   ),

#   tar_target(
#     final_rich_ratio,
#     merge_tile_outputs(
#       files = rich_ratio_tiles,
#       out_file = file.path(merge_dir, "EU_richness_ratio.tif"),
#       wopt = wopt_flt4s()
#     ),
#     format = "file"
#   ),

#   tar_target(
#     final_abc,
#     merge_tile_outputs(
#       files = abc_tiles,
#       out_file = file.path(merge_dir, "EU_abc_shared_loss_gain.tif"),
#       wopt = wopt_int2u()
#     ),
#     format = "file"
#   ),

#   tar_target(
#     final_beta,
#     merge_tile_outputs(
#       files = beta_tiles,
#       out_file = file.path(merge_dir, "EU_beta_Baselga2010_sor_sim_nes.tif"),
#       wopt = wopt_flt4s()
#     ),
#     format = "file"
#   )
)
