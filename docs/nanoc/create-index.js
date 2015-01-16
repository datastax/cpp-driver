var lunr = require('lunr'),
    json = ''

var idx = lunr(function () {
  this.field('title', { boost: 100 })
  this.field('text')
  this.ref('path')
})

process.stdin.setEncoding('utf8');

process.stdin.on('readable', function() {
  var chunk = process.stdin.read();
  if (chunk !== null)
    json += chunk
});

process.stdin.on('end', function() {
  if (json) {
    var items = JSON.parse(json)

    items.forEach(function (item) {
      idx.add(item)
    })
  }

  process.stdout.write(JSON.stringify(idx));
});
