/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function on_load_page() {
  get_request("/overview", on_data, null);
}

function on_data(req, user_data) {
  const overview = JSON.parse(req.responseText);
  var html = "";
  const summary = overview['summary'];
  for (key in summary) {
    html +=`
  <li>
    <ul class="horz">
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
