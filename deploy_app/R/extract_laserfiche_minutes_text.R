source("R/00_config.R")

schertz_laserfiche_minutes_text_cookie_path <- function(cfg = schertz_config) {
  schertz_ensure_dirs(cfg)
  file.path("cache", "laserfiche_cookies.txt")
}

schertz_laserfiche_minutes_text_warm_session <- function(cfg = schertz_config) {
  cookie_path <- schertz_laserfiche_minutes_text_cookie_path(cfg)

  httr2::request("https://laserfiche.schertzweb.com/WebLink/Browse.aspx?id=54085&dbid=1&repo=SCHERTZ") |>
    httr2::req_user_agent(cfg$user_agent) |>
    httr2::req_cookie_preserve(cookie_path) |>
    httr2::req_timeout(cfg$request_seconds) |>
    httr2::req_perform()

  invisible(TRUE)
}

schertz_laserfiche_minutes_text_post <- function(endpoint, body, cfg = schertz_config, max_tries = 5L) {
  cookie_path <- schertz_laserfiche_minutes_text_cookie_path(cfg)
  url <- paste0("https://laserfiche.schertzweb.com/WebLink/DocumentService.aspx/", endpoint)

  last_err <- NULL

  for (i in seq_len(max_tries)) {
    out <- tryCatch(
      {
        Sys.sleep(cfg$sleep_seconds)

        httr2::request(url) |>
          httr2::req_user_agent(cfg$user_agent) |>
          httr2::req_cookie_preserve(cookie_path) |>
          httr2::req_headers(`Content-Type` = "application/json; charset=UTF-8") |>
          httr2::req_body_json(body, auto_unbox = TRUE) |>
          httr2::req_timeout(cfg$request_seconds) |>
          httr2::req_perform() |>
          httr2::resp_body_string()
      },
      error = function(e) {
        last_err <<- e
        NULL
      }
    )

    if (!is.null(out)) {
      return(out)
    }

    message(
      "Laserfiche text retry ", i, "/", max_tries,
      " for ", endpoint, " failed: ",
      conditionMessage(last_err)
    )

    try(schertz_laserfiche_minutes_text_warm_session(cfg = cfg), silent = TRUE)
    Sys.sleep(1.5 * i)
  }

  stop(last_err)
}

schertz_laserfiche_get_text_html_for_page <- function(document_id, page_num, cfg = schertz_config) {
  jsonlite::fromJSON(
    schertz_laserfiche_minutes_text_post(
      "GetTextHtmlForPage",
      list(
        repoName = "SCHERTZ",
        documentId = as.integer(document_id),
        pageNum = as.integer(page_num),
        showAnn = FALSE,
        searchUuid = ""
      ),
      cfg = cfg
    ),
    simplifyDataFrame = FALSE
  )$data$text
}

schertz_extract_laserfiche_plain_text <- function(document_id, page_count, cfg = schertz_config) {
  if (is.na(page_count) || page_count <= 0) {
    return(NA_character_)
  }

  page_text <- vapply(
    seq_len(page_count),
    function(pg) {
      txt <- schertz_laserfiche_get_text_html_for_page(document_id, pg, cfg = cfg)
      txt <- gsub("<[^>]+>", " ", txt)
      txt <- stringr::str_replace_all(txt, "[\r\n\t]+", " ")
      txt <- stringr::str_squish(txt)
      txt
    },
    character(1)
  )

  page_text <- page_text[nzchar(page_text)]

  if (length(page_text) == 0) {
    return(NA_character_)
  }

  paste(page_text, collapse = "\n\n")
}

schertz_add_text_to_laserfiche_minutes <- function(lf_minutes, cfg = schertz_config, only_missing = TRUE) {
  schertz_check_packages(c(
    schertz_required_pipeline_packages,
    "jsonlite"
  ))
  schertz_ensure_dirs(cfg)

  if (nrow(lf_minutes) == 0) {
    return(lf_minutes)
  }

  message("Warming Laserfiche session for plain-text extraction...")
  schertz_laserfiche_minutes_text_warm_session(cfg = cfg)

  out <- lf_minutes

  for (i in seq_len(nrow(out))) {
    if (!identical(out$source_system[i], "laserfiche")) {
      next
    }

    if (only_missing && isTRUE(out$text_available[i])) {
      next
    }

    if (is.na(out$source_record_id[i]) || !nzchar(out$source_record_id[i])) {
      next
    }

    if (is.na(out$n_pages[i]) || out$n_pages[i] <= 0) {
      next
    }

    message("Extracting Laserfiche minute text: ", out$title[i])

    txt <- tryCatch(
      schertz_extract_laserfiche_plain_text(
        document_id = as.integer(out$source_record_id[i]),
        page_count = as.integer(out$n_pages[i]),
        cfg = cfg
      ),
      error = function(e) {
        message("  Extraction failed: ", conditionMessage(e))
        NA_character_
      }
    )

    if (!is.na(txt) && nzchar(txt)) {
      out$text_extracted[i] <- txt
      out$text_available[i] <- TRUE
      out$text_extracted_at[i] <- schertz_now()
    } else {
      out$text_extracted[i] <- NA_character_
      out$text_available[i] <- FALSE
    }
  }

  saveRDS(out, file.path(cfg$derived_dir, "laserfiche_minutes_with_text.rds"))
  readr::write_csv(out, file.path(cfg$derived_dir, "laserfiche_minutes_with_text.csv"))

  out
}
