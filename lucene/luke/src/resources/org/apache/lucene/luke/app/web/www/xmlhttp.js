/*
 * Wrapper function library for XMLHttpRequest (ie AJAX).
 *
 * This library encapsulates the machinery required to set up callbacks to
 * user functions in a manner that is threadsafe (handles multiple
 * simultaneous requests), makes minimal use of global variables (one is
 * required to ensure memory cleanup in IE), and does not leak memory in IE
 * due to lost circular references that cannot be garbage-collected.
 *
 * There are two main entry points, get_request and post_form, which
 * implement HTTP GET and POST, respectively.  post_form is intended for
 * use with an HTML form and encodes its elements as the body of the
 * request in the usual manner.
 *
 * The basic usage pattern is very simple; you call get_request with a url
 * and a callback function you would like to be invoked when the request
 * completes.  Additionally as a convenience you may pass a single argument
 * to be passed along to your callback.
 *
 * IE cleanup: to be totally neat and clean you must call xmlhttp_cleanup()
 * in your onunload handler (usually you would define
 * onunload="xmlhttp_cleanup()" as an attribute of the page's BODY
 * element). It is safe to do this in other browsers too, and might even
 * help their garbage collectors to be more efficient. If you forget to do
 * this, users will leak some memory if they leave the calling page before
 * a request completes.
 *
 */

var xmlhttprequest_stack=[];

// entry point: HTTP GET url; invokes callback on success w/result and userdata
// signature of callback is: callback(req, userdata) where req is the
// XmlHTTP Request object; req.responseText contains the server's response
function get_request (url, callback, userdata)
{
  invoke_request ("GET", url, callback, null, userdata);
}

// entry point: HTTP POST url with form's elements data as body
// invokes callback on success w/result and userdata
// Currently only checkbox and text input form elements are supported
// correctly.
// signature of callback is: callback(req, userdata) where req is the
// XmlHTTP Request object; req.responseText contains the server's response
function post_form (url, callback, form, userdata)
{
   var body = encode_form_data (form);
   invoke_request ("POST", url, callback, body, userdata);
}

function get_form (url, callback, form, userdata)
{
   var query_string = encode_form_data (form);
   invoke_request ("GET", url + "?" + query_string, callback, "", userdata);
}

function invoke_request (method, url, callback, body, userdata)
{
  var req;
  function closure () {
    handle_response (callback, req, userdata);
  }
  /* W3C version */
  if (window.XMLHttpRequest) {
    req = new XMLHttpRequest();
  }
  else if (window.ActiveXObject) {
    req = new ActiveXObject("Microsoft.XMLHTTP");
  }
  if (req) {
    xmlhttprequest_stack.push (req);
    req.onreadystatechange = closure;
    req.open(method, url, true);
    if (method == "POST") {
        req.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
        // Chrome now refuses to allow you to set these?
        // req.setRequestHeader("Content-length", body.length);
        // req.setRequestHeader("Connection", "close");
    }
  }
  req.send(body);
}

function encode_form_data (form)
{
  var encoded_pairs = [];
  for (var i = 0; i < form.elements.length; i++) {
    var element = form.elements[i];
    if (!element || !element.tagName)
      continue;
    var type = element.tagName.toLowerCase();
    var value = null;
    if (type == "input") {
        if (element.type == "hidden" || element.type == "text")
          value = element.value;
        else if (element.type == "checkbox") {
          value = element.checked ? element.value : null;
        }
        else if (element.type == "radio") {
          value = element.checked ? element.value : null;
        }
        else if (element.type == "submit" || element.type == "button" || element.type == "reset") {
          // do nothing
        }
        else {
          alert (element.type + " not currently supported (by xmlhttp.js)");
          // raise an error
          element.type = null;
          return null;
        }
    } else if (type == "textarea") {
        value = element.value;
    } else if (type == "select") {
      value = element.options[element.selectedIndex].value;
    } 
    // else alert (type + " not currently supported");

    if (value) {
      // alert (element.type + " " + element.name + "=" + value);
      encoded_pairs.push (element.name + "=" + escape(value));
    }
  }
  return encoded_pairs.join ("&");
}

function handle_response(callback, req, userdata) {
    // only if req shows "loaded"
    if (req.readyState == 4) {
      // break the circular reference to avoid IE memory leaks!
        xmlhttp_cleanup_req (req);
        if (req.status == 200) {
            // only if "OK"
            callback (req, userdata);
        } else {
            alert("There was a problem retrieving the XML data:\n" +
                  req.statusText + ", status=" + req.status);
        }
    }
}

function empty() {
}

function xmlhttp_cleanup () {
  for (var i in xmlhttprequest_stack) {
    var req = xmlhttprequest_stack[i];
    req.onreadystatechange = empty;
    delete xmlhttprequest_stack[i];
  }
}

function xmlhttp_cleanup_req (req) {
  req.onreadystatechange = empty;
  for (var i=0; i< xmlhttprequest_stack.length; i++) {
    var xreq = xmlhttprequest_stack[i];
    if (req == xreq) {
      delete xmlhttprequest_stack[i];
      break;
    }
  }
}
