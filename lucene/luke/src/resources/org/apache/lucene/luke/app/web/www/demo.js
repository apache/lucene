function on_search(req, user_data) {
  const results = JSON.parse(req.responseText);
  const start = results.start + 1;
  const hits = results.hits;
  const count = results.hits.length;
  var html = "<span id='count'>" + start +
      " to " + (start + count - 1) + " of " + results.total_hits +
      "</span>\n";
  html += "<ol id='hits' start='" + start + "'>\n";
  for (var i = 0; i < count; i++) {
    var hit = hits[i];
    html += "  <li>";
    html += "<span class='scores'>" + hit.docid + "(" + hit.score + ")</span> ";
    var values = hit_values(hit)
    // a lot of these don't exist any more
    //html += "<a href='http://www.amazon.com/dp/" + values['asin']+ "'>" + values['asin'] + "</a>";
    html += "<span class='title' >" + values['title'] + "</span>";
    if (values['description'] || values['bullet_point']) {
      html += "<div><a href='#' onclick='toggle_show_description(event)'> more...</a>";
      html += "<div class='description'><div>" + values['description'] + "</div><div>" + values['bullet_point'] + "</div></div></div>"
    }
    html += "</li>\n";
  }
  html += "</ol>";
  document.getElementById('results').innerHTML = html;
}

function hit_values(hit) {
  // assume there is one value per field
  values = [];
  for (var j = 0; j < hit.field_values.length; j++) {
    var field_value = hit.field_values[j];
    values[field_value.field] = field_value.values[0];
  }
  return values;
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


