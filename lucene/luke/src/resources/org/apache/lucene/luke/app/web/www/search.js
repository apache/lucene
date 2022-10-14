function on_load_page() {
  get_request('/search', on_initial_data, null);
}

function on_initial_data(req, user_data) {
  var html ="<option></option>";
  const response = JSON.parse(req.responseText);
  const field_infos = response.field_infos;
  for (var i = 0; i < field_infos.length; i++) {
    html += `<option>${field_infos[i].name}</option>`;
  }
  document.getElementById('display_field').innerHTML = html;
}

function run() {
  get_form("/search", on_search, document.forms["search"], null);
}

function on_search(req, user_data) {
  //console.log(req.responseText);
  const results = JSON.parse(req.responseText);
  const start = results.start + 1;
  const hits = results.hits;
  const count = results.hits.length;
  const end = count > 0 ? start + count - 1 : 0;
  const display_field = document.getElementById('display_field').value;
  const count_text = count > 0 ? `${start} to ${start + count - 1} of ${results.total_hits}` : "no results";
  var html = `
<span id="count">${count_text}</span>
<ol id="hits" start="${start}">`;
  for (var i = 0; i < count; i++) {
    const hit = hits[i];
    const values = hit.field_values;
    let result_text = "";
    let overflow = "";
    for (var field in values) {
      if (field == display_field ||
          (!display_field && !result_text)) {
        if (field == display_field) {
          result_text = html_encode(values[field][0]);
        } else {
          result_text = `<b>${html_encode(field)}</b> ${html_encode(values[field][0])}`;
        }
        if (values[field].length > 1) {
          overflow += html_encode(values[field].slice(1).toString());
        }
        continue;
      }
      if (values[field].length == 1) {
        overflow += `<b>${html_encode(field)}</b> ${html_encode(values[field][0])} `;
      } else if (values[field].length > 1) {
        overflow += `<b>${html_encode(field)}</b> ${html_encode(values[field].toString())} `;
      }
    }
    if (!result_text) {
      result_text = "<i>no value</i>";
    }
    html += `
  <li>
    <span class="scores">${hit.doc_id} (${hit.score})</span> `;
    html += `<span class="title">${result_text}</span>`;
    if (overflow) {
      html += `
    <div><a href="#" onclick="toggle_show_detail(event); return false;"> more...</a>
      <div class="description">${overflow}</div>
    </div>`;
    }
    html += "  </li>\n";
  }
  html += "</ol>";
  document.getElementById('query').innerHTML = results.parsed_query;
  document.getElementById('results').innerHTML = html;
}

function toggle_show_detail(evt) {
  var desc = evt.target.nextElementSibling;
  console.log(desc.style.display);
  // note - this starts out as '' ?
  var visible = desc.style.display == 'block';
  if (visible) {
    desc.style.display = 'none';
  } else {
    desc.style.display = 'block';
  }
}

