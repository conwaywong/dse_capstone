// See post: http://asmaloney.com/2014/01/code/creating-an-interactive-map-with-leaflet-and-openstreetmap/

var map = L.map( 'map', {
    center: [32.8150, -117.0625],
    minZoom: 2,
    zoom: 11
});

L.tileLayer( 'http://{s}.mqcdn.com/tiles/1.0.0/map/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="http://osm.org/copyright" title="OpenStreetMap" target="_blank">OpenStreetMap</a> contributors | Tiles Courtesy of <a href="http://www.mapquest.com/" title="MapQuest" target="_blank">MapQuest</a> <img src="http://developer.mapquest.com/content/osm/mq_logo.png" width="16" height="16">',
    subdomains: ['otile1','otile2','otile3','otile4']
}).addTo( map );

var myURL = jQuery( 'script[src$="leaf-demo.js"]' ).attr( 'src' ).replace( 'leaf-demo.js', '' );

var myIcon = L.icon({
    iconUrl: myURL + 'images/pin24.png',
    iconRetinaUrl: myURL + 'images/pin48.png',
    iconSize: [29, 24],
    iconAnchor: [9, 21],
    popupAnchor: [0, -14]
});


// for ( var i=0; i < markers.length; ++i )
// {
//    L.marker( [markers[i].lat, markers[i].lng], {icon: myIcon} )
//       .bindPopup( '<a href="' + markers[i].url + '" target="_blank">' + markers[i].name + '</a>' )
//       .addTo( map );
// }
var heatPoints = [];
for ( var i=0; i < markers.length; ++i ) {
  heatPoints.push(L.latLng(markers[i].lat, markers[i].lng));
}

var heat = L.heatLayer(heatPoints, {radius: 30, blur: 24, gradient: {0.3: 'yellow', 0.7: 'green', 1: 'red'}}).addTo(map);
