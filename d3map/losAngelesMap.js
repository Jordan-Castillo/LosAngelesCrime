const width = 960;
const height = 960;
const svg = d3.select('body').append('svg')
  .attr('height', width)
  .attr('width', height);

d3.queue()
    .defer(d3.json, 'la.json')
  .await(render)

function render(error, la) {
  if (error) return console.warn(error);

  // Create a unit projection.
  const laProjection = d3.geoAlbers()
                          .scale(1)
                          .translate([0, 0]);

  // Create a path generator                              
  const path = d3.geoPath()
                    .projection(laProjection);

  // Compute the bounds of a feature of interest, 
  // then derive scale & translate.
  const laBounds = path.bounds(la);
  const laScale = 0.95 / Math.max(
            (laBounds[1][0] - laBounds[0][0]) / width,
            (laBounds[1][1] - laBounds[0][1]) / height
          );
  const laTranslate = [
    (width - laScale * (laBounds[1][0] + laBounds[0][0])) / 2, 
    (height - laScale * (laBounds[1][1] + laBounds[0][1])) / 2
  ];

  // Update the projection to use computed scale & translate.
  console.log(laBounds);
  console.log(laScale, laTranslate)
  laProjection.scale(laScale)
              .translate(laTranslate);

  svg.append('path')
      .datum(la)
      .attr('d', path);
}