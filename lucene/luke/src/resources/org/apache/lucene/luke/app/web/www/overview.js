function on_load() {
  get_request("/overview", on_data, null);
}

function on_data(req, user_data) {
  const overview = JSON.parse(req.responseText);
  var html = "";
  const summary = overview['summary'];
  for (key in summary) {
    html +=`
  <li>
    <ul>
      <li class="label">${key}</li>
      <li>${summary[key]}</li>
    </ul>
  </li>`
  }
  document.getElementById('overview').innerHTML = html;
  const termFields = overview['term_fields'];
  const fieldDocCount = overview['field_doc_count'];
  html = `
  <tr>
    <th>Name</th>
    <th>Term count</th>
    <th>Doc count</th>
  </tr>`;
  for (field in termFields) {
    html += `
  <tr>
    <td><a href="#" onclick="select_field('${field}')">${field}</a></td>
    <td class="number">${termFields[field]}</td>
    <td class="number">${fieldDocCount[field]}</td>
  </tr>`;
  }
  document.getElementById('term_fields').innerHTML = html;
}


function select_field(field) {
  get_request("/terms?field=" + encodeURIComponent(field), on_field, null);
}

function on_field(req, user_data) {
  const response = JSON.parse(req.responseText);
  var html = "";
  for (var i = 0; i < response.length; i++) {
    html += `
<li>
  <span class="count">${response[i].doc_freq}</span>
  <span>${response[i].decoded_term_text}</span>
</li>`;
  }
  document.getElementById('field_terms').innerHTML = html;
}
