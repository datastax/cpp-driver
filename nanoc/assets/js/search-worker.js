importScripts('lunr.js');

var index = lunr.Index.load(<%= lunr_index %>);

// The worker receives a message everytime the web app wants to query the index
onmessage = function(oEvent) {
  var q = oEvent.data.q;
  var hits = index.search(q);
  var results = [];
  // Only return the array of paths to pages
  hits.forEach(function(hit) {
    results.push(hit);
  });
  // The results of the query are sent back to the web app via a new message
  postMessage({ e: 'query-ready', q: q, d: results });
};

postMessage({ e: 'index-ready' });
