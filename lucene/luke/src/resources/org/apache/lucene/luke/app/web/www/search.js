/**
 * Convert a string to HTML entities
 */
function html_encode(str) {
    return str.replace(/[<>"]/gm, function(s) {
      return "&#" + s.charCodeAt(0) + ";";
    });
};

function on_search(req, user_data) {
  console.log(req.responseText);
  const results = JSON.parse(req.responseText);
  const start = results.start + 1;
  const hits = results.hits;
  const count = results.hits.length;
  var html = `
<span id="count">${start} to ${start + count - 1} of ${results.total_hits}</span>
<ol id="hits" start="${start}">`;
  for (var i = 0; i < count; i++) {
    var hit = hits[i];
    html += `
  <li>
    <span class="scores">${hit.doc_id} (${hit.score})</span> `;
    var values = hit.field_values;
    html += `<span class="title">${values['title']}</span>`;
    if (values['description'] || values['bullet_point']) {
      html += `
    <div><a href="#" onclick="toggle_show_description(event); return false;"> more...</a>
      <div class="description">
        <div>${html_encode(values["description"][0])}</div>
        <div>${html_encode(values["bullet_point"][0])}</div>
      </div>
    </div>`;
    }
    html += "</li>\n";
  }
  html += "</ol>";
  document.getElementById('query').innerHTML = results.parsed_query;
  document.getElementById('results').innerHTML = html;
}

function run() {
  get_form("/search", on_search, document.forms["search"], null);
}

function toggle_show_description(evt) {
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

