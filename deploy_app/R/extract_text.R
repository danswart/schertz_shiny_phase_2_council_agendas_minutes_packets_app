source("R/00_config.R")

schertz_extract_pdf_text <- function(path) {
  txt <- pdftools::pdf_text(path)
  txt <- stringr::str_squish(txt)
  list(
    text = paste(txt, collapse = "\n\n"),
    pages = length(txt)
  )
}

schertz_add_text_to_documents <- function(docs, cfg = schertz_config, only_missing = TRUE) {
  schertz_check_packages(schertz_required_pipeline_packages)
  schertz_ensure_dirs(cfg)

  if (nrow(docs) == 0) {
    return(docs)
  }

  for (i in seq_len(nrow(docs))) {
    local_file <- docs$local_file[i]

    if (is.na(local_file) || !nzchar(local_file) || !file.exists(local_file)) {
      next
    }

    if (isTRUE(only_missing) && isTRUE(docs$text_available[i])) {
      next
    }

    message("Extracting text: ", basename(local_file))

    res <- tryCatch({
      schertz_extract_pdf_text(local_file)
    }, error = function(e) {
      message("  Text extraction failed: ", conditionMessage(e))
      NULL
    })

    if (!is.null(res)) {
      docs$text_extracted[i] <- res$text
      docs$text_available[i] <- nzchar(res$text)
      docs$n_pages[i] <- res$pages
      docs$text_extracted_at[i] <- schertz_now()
    }
  }

  saveRDS(docs, file.path(cfg$derived_dir, "documents_with_text.rds"))
  readr::write_csv(docs, file.path(cfg$derived_dir, "documents_with_text.csv"))
  docs
}
