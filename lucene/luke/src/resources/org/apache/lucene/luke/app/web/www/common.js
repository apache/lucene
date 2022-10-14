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
