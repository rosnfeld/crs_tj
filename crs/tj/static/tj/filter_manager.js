/*
 * Trying to separate JavaScript code for dealing with filters.
 * Only partially successful so far.
 */

var CODE_FILTER_MAP = {};

var GET_FILTERS_URL; // to be set externally to take advantage of django url-building

var UPDATE_FILTERS_URL_MAP;

function setFilterUrls(get_url, update_url_map) {
    GET_FILTERS_URL = get_url;
    UPDATE_FILTERS_URL_MAP = update_url_map;
}

function refreshCodeFilters(additional_callback) {
    $.ajax({
        url: GET_FILTERS_URL,

        type: "GET",

        success: function (json) {
            CODE_FILTER_MAP = json;

            if (additional_callback != null) {
                additional_callback();
            }
        },

        error: function (xhr, status) {
            alert("Failed to refresh code filters");
        }
    });
}

function getFilterTypes() {
    return Object.keys(CODE_FILTER_MAP);
}

function isFilterActive(filter_type) {
    return CODE_FILTER_MAP[filter_type].length > 0;
}

function getFilterCodes(filter_type) {
    return CODE_FILTER_MAP[filter_type];
}
