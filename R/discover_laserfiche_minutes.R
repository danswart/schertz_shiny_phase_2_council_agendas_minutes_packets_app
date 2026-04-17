source("R/00_config.R")

schertz_laserfiche_minutes_root_id <- function() {
  54085L
}

schertz_laserfiche_cookie_path <- function(cfg = schertz_config) {
  schertz_ensure_dirs(cfg)
  file.path("cache", "laserfiche_cookies.txt")
}

schertz_laserfiche_warm_session <- function(cfg = schertz_config) {
  cookie_path <- schertz_laserfiche_cookie_path(cfg)

  httr2::request("https://laserfiche.schertzweb.com/WebLink/Browse.aspx?id=54085&dbid=1&repo=SCHERTZ") |>
    httr2::req_user_agent(cfg$user_agent) |>
    httr2::req_cookie_preserve(cookie_path) |>
    httr2::req_timeout(cfg$request_seconds) |>
    httr2::req_perform()

  invisible(TRUE)
}

schertz_laserfiche_service_post <- function(endpoint, body, cfg = schertz_config, max_tries = 5L) {
  cookie_path <- schertz_laserfiche_cookie_path(cfg)
  url <- paste0(
    "https://laserfiche.schertzweb.com/WebLink/FolderListingService.aspx/",
    endpoint
  )

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
      "Laserfiche retry ", i, "/", max_tries,
      " for ", endpoint, " failed: ",
      conditionMessage(last_err)
    )

    try(schertz_laserfiche_warm_session(cfg = cfg), silent = TRUE)
    Sys.sleep(1.5 * i)
  }

  stop(last_err)
}

schertz_laserfiche_get_folder_listing2 <- function(folder_id, start = 0L, end = 200L, cfg = schertz_config) {
  jsonlite::fromJSON(
    schertz_laserfiche_service_post(
      "GetFolderListing2",
      list(
        repoName = "SCHERTZ",
        folderId = as.integer(folder_id),
        getNewListing = TRUE,
        start = as.integer(start),
        end = as.integer(end),
        sortColumn = "",
        sortAscending = TRUE
      ),
      cfg = cfg
    ),
    simplifyDataFrame = TRUE
  )
}

schertz_laserfiche_get_folder_listing_ids <- function(folder_id, cfg = schertz_config) {
  jsonlite::fromJSON(
    schertz_laserfiche_service_post(
      "GetFolderListingIds",
      list(
        repoName = "SCHERTZ",
        folderId = as.integer(folder_id),
        sortColumn = "",
        sortAscending = TRUE
      ),
      cfg = cfg
    ),
    simplifyVector = TRUE
  )$data
}

schertz_laserfiche_get_breadcrumbs <- function(folder_id, cfg = schertz_config) {
  jsonlite::fromJSON(
    schertz_laserfiche_service_post(
      "GetBreadCrumbs",
      list(
        vdirName = "WebLink",
        repoName = "SCHERTZ",
        folderId = as.integer(folder_id)
      ),
      cfg = cfg
    ),
    simplifyDataFrame = TRUE
  )$data
}

schertz_laserfiche_get_document_info <- function(entry_id, cfg = schertz_config) {
  jsonlite::fromJSON(
    schertz_laserfiche_service_post(
      "GetDocumentInfo",
      list(
        repoName = "SCHERTZ",
        dId = as.integer(entry_id)
      ),
      cfg = cfg
    ),
    simplifyDataFrame = FALSE
  )$data
}

schertz_extract_laserfiche_date <- function(x) {
  if (length(x) == 0 || is.na(x) || !nzchar(x)) {
    return(as.Date(NA))
  }

  x <- stringr::str_squish(x)

  candidates <- c(
    stringr::str_extract(x, "\\d{2}-\\d{2}-\\d{4}"),
    stringr::str_extract(x, "\\d{2}-\\d{2}-\\d{2}"),
    stringr::str_extract(x, "[A-Za-z]+\\s+\\d{1,2},\\s+\\d{4}")
  )
  candidates <- candidates[!is.na(candidates)]

  for (cand in candidates) {
    parsed <- suppressWarnings(lubridate::mdy(cand))
    if (!is.na(parsed)) {
      return(as.Date(parsed))
    }
  }

  as.Date(NA)
}

schertz_laserfiche_meeting_type <- function(x) {
  target <- stringr::str_to_lower(dplyr::coalesce(x, ""))

  dplyr::case_when(
    stringr::str_detect(target, "workshop") ~ "workshop",
    stringr::str_detect(target, "special") ~ "special",
    stringr::str_detect(target, "canvass|canvassing") ~ "special",
    stringr::str_detect(target, "regular|minutes") ~ "regular",
    TRUE ~ "other"
  )
}

schertz_empty_laserfiche_minutes <- function() {
  tibble::tibble(
    doc_id = character(),
    source_system = character(),
    source_category = character(),
    meeting_body = character(),
    meeting_type = character(),
    meeting_date = as.Date(character()),
    fiscal_year = integer(),
    doc_type = character(),
    title = character(),
    source_page_url = character(),
    document_url = character(),
    pdf_url = character(),
    source_record_id = character(),
    file_name = character(),
    local_file = character(),
    text_extracted = character(),
    text_available = logical(),
    n_pages = integer(),
    discovered_at = as.POSIXct(character()),
    downloaded_at = as.POSIXct(character()),
    text_extracted_at = as.POSIXct(character()),
    is_active = logical(),
    notes = character()
  )
}

schertz_discover_laserfiche_minutes <- function(cfg = schertz_config) {
  schertz_check_packages(c(
    schertz_required_pipeline_packages,
    "jsonlite"
  ))
  schertz_ensure_dirs(cfg)

  message("Warming Laserfiche session...")
  schertz_laserfiche_warm_session(cfg = cfg)

  root_id <- schertz_laserfiche_minutes_root_id()

  message("Reading Laserfiche Minutes root folder...")
  first_page <- schertz_laserfiche_get_folder_listing2(
    root_id,
    start = 0L,
    end = 200L,
    cfg = cfg
  )

  visible_folders <- tibble::as_tibble(first_page$data$results) |>
    dplyr::filter(!is.na(.data$entryId)) |>
    dplyr::transmute(
      folder_name = .data$name,
      folder_id = as.integer(.data$entryId)
    )

  all_folder_ids <- schertz_laserfiche_get_folder_listing_ids(root_id, cfg = cfg)

  hidden_folder_ids <- setdiff(all_folder_ids, as.character(visible_folders$folder_id))

  hidden_folders <- if (length(hidden_folder_ids) > 0) {
    purrr::map_dfr(
      hidden_folder_ids,
      function(id) {
        bc <- schertz_laserfiche_get_breadcrumbs(as.integer(id), cfg = cfg)
        tibble::tibble(
          folder_name = tail(bc$name, 1),
          folder_id = as.integer(id)
        )
      }
    )
  } else {
    tibble::tibble(
      folder_name = character(),
      folder_id = integer()
    )
  }

  fiscal_year_folders <- dplyr::bind_rows(visible_folders, hidden_folders) |>
    dplyr::distinct(.data$folder_id, .keep_all = TRUE) |>
    dplyr::arrange(.data$folder_name)

  saveRDS(
    fiscal_year_folders,
    file.path(cfg$derived_dir, "laserfiche_minutes_fiscal_year_folders.rds")
  )
  readr::write_csv(
    fiscal_year_folders,
    file.path(cfg$derived_dir, "laserfiche_minutes_fiscal_year_folders.csv")
  )

  message("Reading minute records from each fiscal-year folder...")
  minute_rows <- purrr::map_dfr(
    fiscal_year_folders$folder_id,
    function(fid) {
      listing <- schertz_laserfiche_get_folder_listing2(
        fid,
        start = 0L,
        end = 200L,
        cfg = cfg
      )

      tibble::as_tibble(listing$data$results) |>
        dplyr::filter(!is.na(.data$entryId), .data$type == -2) |>
        dplyr::transmute(
          fiscal_year_folder_id = as.integer(fid),
          title = .data$name,
          entryId = as.integer(.data$entryId),
          extension = dplyr::coalesce(.data$extension, ""),
          isEdoc_from_listing = dplyr::coalesce(.data$isEdoc, FALSE)
        )
    }
  )

  if (nrow(minute_rows) == 0) {
    out <- schertz_empty_laserfiche_minutes()
    saveRDS(out, file.path(cfg$derived_dir, "laserfiche_minutes_documents.rds"))
    readr::write_csv(out, file.path(cfg$derived_dir, "laserfiche_minutes_documents.csv"))
    return(out)
  }

  message("Checking document info and keeping only edoc-backed minutes...")
  minute_rows <- minute_rows |>
    dplyr::mutate(
      doc_info = purrr::map(
        .data$entryId,
        function(id) {
          schertz_laserfiche_get_document_info(id, cfg = cfg)
        }
      ),
      page_count = purrr::map_int(.data$doc_info, ~ dplyr::coalesce(.x$pageCount, 0L)),
      is_edoc = purrr::map_lgl(.data$doc_info, ~ isTRUE(.x$isEdoc))
    )

  saveRDS(
    minute_rows,
    file.path(cfg$derived_dir, "laserfiche_minutes_raw_rows.rds")
  )
  readr::write_csv(
    minute_rows |>
      dplyr::select(-.data$doc_info),
    file.path(cfg$derived_dir, "laserfiche_minutes_raw_rows.csv")
  )

  out <- minute_rows |>
    dplyr::filter(.data$is_edoc) |>
    dplyr::transmute(
      doc_id = schertz_make_doc_id("laserfiche", as.character(.data$entryId), "minutes"),
      source_system = "laserfiche",
      source_category = "archive_minutes",
      meeting_body = cfg$meeting_body,
      meeting_type = schertz_laserfiche_meeting_type(.data$title),
      meeting_date = vapply(.data$title, schertz_extract_laserfiche_date, as.Date(NA)),
      fiscal_year = schertz_guess_fiscal_year(
        vapply(.data$title, schertz_extract_laserfiche_date, as.Date(NA))
      ),
      doc_type = "minutes",
      title = .data$title,
      source_page_url = paste0(
        "https://laserfiche.schertzweb.com/WebLink/Browse.aspx?id=",
        .data$fiscal_year_folder_id,
        "&dbid=1&repo=SCHERTZ"
      ),
      document_url = paste0(
        "https://laserfiche.schertzweb.com/WebLink/DocView.aspx?id=",
        .data$entryId,
        "&dbid=1&repo=SCHERTZ"
      ),
      pdf_url = "",
      source_record_id = as.character(.data$entryId),
      file_name = paste0(.data$entryId, ".pdf"),
      local_file = NA_character_,
      text_extracted = NA_character_,
      text_available = FALSE,
      n_pages = .data$page_count,
      discovered_at = schertz_now(),
      downloaded_at = as.POSIXct(NA),
      text_extracted_at = as.POSIXct(NA),
      is_active = TRUE,
      notes = "Laserfiche archive minute retained because GetDocumentInfo reported isEdoc = TRUE"
    ) |>
    dplyr::arrange(dplyr::desc(.data$meeting_date), .data$title)

  saveRDS(out, file.path(cfg$derived_dir, "laserfiche_minutes_documents.rds"))
  readr::write_csv(out, file.path(cfg$derived_dir, "laserfiche_minutes_documents.csv"))

  out
}

