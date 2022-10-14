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
/**
 * Every page should initialize by calling this, and define its own on_load_page()
 * to perform its own initialization 
 */
function on_load(current_path) {
  const nav = [
    ['Overview', '/www/index.html'],
    ['Search', '/www/search.html']
  ];
  var html = "";
  for (var i = 0; i < nav.length; i++) {
    var page_name = nav[i][0];
    var page_path = nav[i][1];
    if (page_path == current_path) {
      html += `<li class="selected">${page_name}</li>`;
    } else {
      html += `<li><a href="${page_path}">${page_name}</a></li>`;
    }
  }
  document.getElementById('nav').innerHTML = html;
  on_load_page();
}

/**
 * Convert a string to HTML entities
 */
function html_encode(str) {
  if (str) {
    return str.replace(/[<>"]/gm, function(s) {
      return "&#" + s.charCodeAt(0) + ";";
    });
  } else {
    return str;
  }
};
