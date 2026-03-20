# R/functions_tile_beta.R

suppressPackageStartupMessages({
  library(terra)
})

# ---------------------------------------------------------
# Global terra / GDAL settings helper
# ---------------------------------------------------------
set_terra_hpc <- function() {
  terra::terraOptions(
    memfrac = 0.8,
    progress = 0,
    threads = 1
  )
  Sys.setenv(
    GDAL_NUM_THREADS = "1",
    OMP_NUM_THREADS  = "1"
  )
  invisible(TRUE)
}

# ---------------------------------------------------------
# Convert filenames / layer names to comparable species keys
# ---------------------------------------------------------
to_species_key <- function(x) {
  x <- basename(x)
  x <- sub("\\.tif$", "", x, ignore.case = TRUE)
  x <- sub("^binary[_ ]+", "", x, ignore.case = TRUE)

  # remove current suffixes
  x <- sub("(_CurrentActual|_CurrentAtual|_Current)$", "", x, ignore.case = TRUE)

  # remove future suffixes
  x <- sub(
    "(_FuturePotentialReachable|_FuturePotential|_FutureReachable|_Future)$",
    "",
    x,
    ignore.case = TRUE
  )

  x <- gsub("[[:space:]]+", "_", x)
  x <- gsub("_+", "_", x)
  x <- gsub("^_|_$", "", x)

  x
}

# ---------------------------------------------------------
# Extract numeric tile ID from file path
# Works for names like tile_current12.tif / tile_future12.tif
# ---------------------------------------------------------
get_tile_id <- function(x) {
  sub("^.*?(\\d+)\\.tif$", "\\1", basename(x))
}

# ---------------------------------------------------------
# Match current and future tile files by tile ID
# Returns a data.frame with one row per matched tile pair
# ---------------------------------------------------------
match_tile_files <- function(cur_tile_dir, fut_tile_dir) {
  cur_files <- list.files(cur_tile_dir, pattern = "\\.tif$", full.names = TRUE)
  fut_files <- list.files(fut_tile_dir, pattern = "\\.tif$", full.names = TRUE)

  if (length(cur_files) == 0) {
    stop("No current tile files found in: ", cur_tile_dir)
  }
  if (length(fut_files) == 0) {
    stop("No future tile files found in: ", fut_tile_dir)
  }

  cur_ids <- get_tile_id(cur_files)
  fut_ids <- get_tile_id(fut_files)

  if (!setequal(cur_ids, fut_ids)) {
    stop(
      "Tile sets differ.\n",
      "Missing in future: ", paste(setdiff(cur_ids, fut_ids), collapse = ", "), "\n",
      "Missing in current: ", paste(setdiff(fut_ids, cur_ids), collapse = ", ")
    )
  }

  ord <- order(as.integer(cur_ids))
  cur_files <- cur_files[ord]
  cur_ids   <- cur_ids[ord]

  fut_files <- fut_files[match(cur_ids, fut_ids)]

  data.frame(
    tile_id  = cur_ids,
    cur_tile = cur_files,
    fut_tile = fut_files,
    stringsAsFactors = FALSE
  )
}

# ---------------------------------------------------------
# Align future tile layer order to current tile layer order
# based on species names
# ---------------------------------------------------------
match_tile_layers <- function(C, F, tile_id = NA_character_) {
  cur_key <- to_species_key(names(C))
  fut_key <- to_species_key(names(F))

  if (!setequal(cur_key, fut_key)) {
    stop(
      "Species sets differ in tile ", tile_id, ".\n",
      "Missing in future: ", paste(setdiff(cur_key, fut_key), collapse = ", "), "\n",
      "Missing in current: ", paste(setdiff(fut_key, cur_key), collapse = ", ")
    )
  }

  F <- F[[match(cur_key, fut_key)]]
  names(F) <- names(C)

  list(C = C, F = F, species = cur_key)
}

# ---------------------------------------------------------
# Standard write options
# ---------------------------------------------------------
wopt_int2u <- function() {
  list(
    datatype = "INT2U",
    gdal = c("COMPRESS=LZW", "TILED=YES", "BIGTIFF=IF_SAFER")
  )
}

wopt_int2s <- function() {
  list(
    datatype = "INT2S",
    gdal = c("COMPRESS=LZW", "TILED=YES", "BIGTIFF=IF_SAFER")
  )
}

wopt_flt4s <- function() {
  list(
    datatype = "FLT4S",
    gdal = c("COMPRESS=LZW", "TILED=YES", "BIGTIFF=IF_SAFER")
  )
}

# ---------------------------------------------------------
# Merge non-overlapping tile rasters into a full raster
# ---------------------------------------------------------
merge_tile_outputs <- function(files,
                               out_file,
                               overwrite = TRUE,
                               wopt = NULL) {
  set_terra_hpc()

  if (length(files) == 0) {
    stop("No files supplied to merge_tile_outputs().")
  }

  rlist <- lapply(files, terra::rast)
  out <- do.call(terra::merge, rlist)

  if (is.null(wopt)) {
    terra::writeRaster(out, out_file, overwrite = overwrite)
  } else {
    terra::writeRaster(out, out_file, overwrite = overwrite, wopt = wopt)
  }

  out_file
}

# ---------------------------------------------------------
# Process one tile pair
#
# Outputs:
# - current richness
# - future richness
# - richness change
# - a_shared, b_losses, c_gains
# - beta_sor, beta_sim, beta_nes
#
# Returns a named list of output files
# ---------------------------------------------------------
process_one_tile_beta <- function(cur_tile,
                                  fut_tile,
                                  out_dir,
                                  overwrite = TRUE) {
  set_terra_hpc()

  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  tile_id_cur <- get_tile_id(cur_tile)
  tile_id_fut <- get_tile_id(fut_tile)

  if (!identical(tile_id_cur, tile_id_fut)) {
    stop("Tile ID mismatch: current=", tile_id_cur, ", future=", tile_id_fut)
  }

  tile_id <- tile_id_cur
  message("Processing tile ", tile_id)

  # -----------------------------
  # Read tile stacks
  # -----------------------------
  C <- terra::rast(cur_tile)
  F <- terra::rast(fut_tile)

  if (!terra::compareGeom(C, F, stopOnError = FALSE)) {
    stop("Current and future tile geometry do not match for tile ", tile_id)
  }

  matched <- match_tile_layers(C, F, tile_id = tile_id)
  C <- matched$C
  F <- matched$F

  # -----------------------------
  # Richness
  # -----------------------------
  rich_cur <- sum(C == 1, na.rm = TRUE)
  rich_fut <- sum(F == 1, na.rm = TRUE)

  # -----------------------------
  # Shared / losses / gains
  # -----------------------------
  a_shared <- sum((C == 1) & (F == 1), na.rm = TRUE)

  # # (Not sure if I need codes below)cells are NA only if both current and future are fully NA at that cell
  # nonNA_cur <- sum(!is.na(C))
  # nonNA_fut <- sum(!is.na(F))
  # both_all_na <- (nonNA_cur == 0) & (nonNA_fut == 0)
  # 
  # rich_cur[both_all_na] <- NA
  # rich_fut[both_all_na] <- NA
  # a_shared[both_all_na] <- NA

  delta_rich <- rich_fut - rich_cur

# proportional richness change: (future - current) / current
ratio_rich <- terra::ifel(rich_cur > 0, delta_rich / rich_cur, NA)
ratio_rich <- terra::ifel(rich_cur == 0 & rich_fut == 0, 0, ratio_rich)
# # colonization from zero-richness baseline
# colonization_rich <- terra::ifel(rich_cur == 0 & rich_fut > 0, rich_fut, 0)

  
  b_losses   <- rich_cur - a_shared
  c_gains    <- rich_fut - a_shared
  
  # Same as the reason above, it seems I do not need to compute the scripts below,
  # it will cost me extra computing time, from the test the tile looks OK.
  
  # b_losses[both_all_na]   <- NA
  # c_gains[both_all_na]    <- NA
  # delta_rich[both_all_na] <- NA

  # Guard against tiny negative artifacts
  b_losses <- terra::ifel(b_losses < 0, 0, b_losses)
  c_gains  <- terra::ifel(c_gains  < 0, 0, c_gains)

  abc <- c(a_shared, b_losses, c_gains)
  names(abc) <- c("a_shared", "b_losses", "c_gains")

  # -----------------------------
  # Baselga 2010 beta diversity
  # -----------------------------
  beta_stack <- terra::lapp(
    abc,
    fun = function(a, b, c) {
      # na_any <- is.na(a) | is.na(b) | is.na(c)

      den_sor  <- 2 * a + b + c
      beta_sor <- ifelse(den_sor == 0, 0, (b + c) / den_sor)
      
     # Simpson dissimilarity (turnover component of Sorensen). 
      m        <- pmin(b, c)
      den_sim  <- a + m
      beta_sim <- ifelse(den_sim == 0, 0, m / den_sim)

      beta_nes <- beta_sor - beta_sim

      # beta_sor[na_any] <- NA_real_
      # beta_sim[na_any] <- NA_real_
      # beta_nes[na_any] <- NA_real_
# Higher beta sorensen means higher dissimilarity and lower microrefugia potential.
# beta simpson is the turnover component of beta sorensen, so higher beta simpson means higher turnover.
# Beta nestedness is the nestedness component of beta sorensen, so higher beta nestedness means higher nestedness (i.e. more species loss without replacement)
      beta_sor <- pmax(0, pmin(1, beta_sor))
      beta_sim <- pmax(0, pmin(1, beta_sim))
      beta_nes <- pmax(0, pmin(1, beta_nes))

      cbind(beta_sor, beta_sim, beta_nes)
    }
  )
  names(beta_stack) <- c("beta_sor", "beta_sim", "beta_nes")

  # -----------------------------
  # Output paths
  # -----------------------------
  f_rich_cur <- file.path(out_dir, paste0("tile_", tile_id, "_richness_current.tif"))
  f_rich_fut <- file.path(out_dir, paste0("tile_", tile_id, "_richness_future.tif"))
  f_delta    <- file.path(out_dir, paste0("tile_", tile_id, "_richness_change.tif"))
  f_abc      <- file.path(out_dir, paste0("tile_", tile_id, "_abc.tif"))
  f_beta     <- file.path(out_dir, paste0("tile_", tile_id, "_beta.tif"))
  f_ratio <- file.path(out_dir, paste0("tile_", tile_id, "_richness_ratio.tif"))
  #f_colonization  <- file.path(out_dir, paste0("tile_", tile_id, "_colonization_richness.tif"))

  # -----------------------------
  # Write outputs
  # -----------------------------
  terra::writeRaster(rich_cur, f_rich_cur, overwrite = overwrite, wopt = wopt_int2u())
  terra::writeRaster(rich_fut, f_rich_fut, overwrite = overwrite, wopt = wopt_int2u())
  terra::writeRaster(delta_rich, f_delta, overwrite = overwrite, wopt = wopt_int2s())
  terra::writeRaster(abc, f_abc, overwrite = overwrite, wopt = wopt_int2u())
  terra::writeRaster(beta_stack, f_beta, overwrite = overwrite, wopt = wopt_flt4s())
  terra::writeRaster(ratio_rich, f_ratio, overwrite = overwrite, wopt = wopt_flt4s())
  #terra::writeRaster(colonization_rich, f_colonization, overwrite = overwrite, wopt = wopt_int2u())

  list(
    tile_id     = tile_id,
    rich_cur    = f_rich_cur,
    rich_fut    = f_rich_fut,
    rich_change = f_delta,
    rich_ratio  = f_ratio,
    #colonization_rich  = f_colonization,
    abc         = f_abc,
    beta        = f_beta
  )
}