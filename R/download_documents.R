source("R/00_config.R")

schertz_local_filename <- function(doc) {
  ext <- tools::file_ext(doc$pdf_url)
  if (is.na(ext) || !nzchar(ext)) {
    ext <- "pdf"
  }

  paste0(gsub("[^A-Za-z0-9_-]", "_", doc$doc_id), ".", ext)
}

schertz_download_documents <- function(docs, cfg = schertz_config, only_missing = TRUE) {
  schertz_check_packages(schertz_required_pipeline_packages)
  schertz_ensure_dirs(cfg)

  if (nrow(docs) == 0) {
    return(docs)
  }

  for (i in seq_len(nrow(docs))) {
    if (!nzchar(docs$pdf_url[i])) {
      next
    }

    if (isTRUE(only_missing) && !is.na(docs$local_file[i]) && nzchar(docs$local_file[i]) && file.exists(docs$local_file[i])) {
      next
    }

    local_name <- schertz_local_filename(docs[i, ])
    destfile <- file.path(cfg$raw_dir, local_name)

    message("Downloading: ", docs$title[i])

    ok <- tryCatch({
      schertz_download_binary(docs$pdf_url[i], destfile, cfg = cfg)
      TRUE
    }, error = function(e) {
      message("  Download failed: ", conditionMessage(e))
      FALSE
    })

    if (isTRUE(ok)) {
      docs$local_file[i] <- destfile
      docs$downloaded_at[i] <- schertz_now()
    }

    Sys.sleep(cfg$sleep_seconds)
  }

  saveRDS(docs, file.path(cfg$derived_dir, "documents_downloaded.rds"))
  readr::write_csv(docs, file.path(cfg$derived_dir, "documents_downloaded.csv"))
  docs
}
