import { Base } from "./Base.js";

/**
 * Based on js class from Lab 5
 * https://github.com/Harvard-DCE-BIWADA/S14A2022/blob/main/lab5/static/js/bar.js
 **/

export class Histogram extends Base {
  histogram = d3.histogram();

  constructor(_target, _data, _options, _histogram_options) {
    super(_target, _data);
    this.options = _options;

    this.init();
  }

  init() {
    // Define this vis
    const vis = this;

    super.init();

    // shift the graph origin
    vis.g.style(
      "transform",
      `translate(${vis.gMargin.left + 15}px, ${vis.gMargin.top - 15}px)`
    );

    // my init
    vis.histogram = d3.histogram();
    vis.scX = d3.scaleLinear().range([0, vis.gW]);
    vis.scY = d3.scaleLinear().range([0, vis.gH]);

    // Setup X Axis
    vis.axes.x.scale = d3.scaleLinear().range([vis.gW, 0]);
    vis.axes.x.group = vis.g
      .append("g")
      .attr("class", "axis axisX")
      .style("transform", `translateY(${vis.gW + 15}px`);

    // Setup Y Axis
    vis.axes.y.scale = d3.scaleLinear().range([vis.gH, 0]);
    vis.axes.y.group = vis.g
      .append("g")
      .attr("class", "axis axisY")
      .style("transform", `translateX(${-15}px`);

    vis.wrangle();
  }

  wrangle() {
    // Define this vis
    const vis = this;

    const data_mapper = (d) => d;
    const data_mapped = vis.data.map(data_mapper);
    if (vis.options?.thresholds) {
      vis.histogram.thresholds(vis.options.thresholds);
    }
    vis.data_bins = vis.histogram(data_mapped);

    console.log(vis.data_bins);
    vis.scX.domain(d3.extent(data_mapped, data_mapper));
    vis.scY.domain([0, d3.max(vis.data_bins, (d) => d.length)]);

    // Axes Setup
    vis.axes.x.link
      .scale(vis.scX)
      .ticks(vis.data_bins.length)
      .tickFormat(this.options?.xFormat ?? ((label) => label));

    // we reversed the scales already, so let's just set the domain again.
    vis.axes.y.scale.domain([0, d3.max(vis.data_bins, (d) => d.length)]);
    vis.axes.y.link
      .scale(vis.axes.y.scale)
      .tickFormat(this.options?.yFormat ?? ((label) => label));

    // Now render
    vis.render();
  }

  render() {
    // Define this vis
    const vis = this;

    // update axes
    vis.axes.x.group
      .call(vis.axes.x.link)
      .append("text") // this x
      .attr("class", "label")
      .attr("text-anchor", "middle")
      .attr("x", vis.gW / 2)
      .attr("dy", "2.5em")
      .text(this.options.xLabel ?? "");

    vis.axes.y.group
      .call(vis.axes.y.link)
      .append("text")
      .attr("class", "label")
      .attr("text-anchor", "middle")
      .attr("transform", "rotate(-90)")
      .attr("x", -(vis.gH / 2))
      .attr("y", -vis.gMargin.left)
      .attr("dy", "1em")
      .text(this.options.yLabel ?? "");

    const w = Math.round(vis.gW / vis.data_bins.length);

    let group = vis.g
      .append("g")
      .attr("class", "barG")
      .attr("transform", ` translate( 0, ${vis.gH}) scale(1,-1)`);

    group
      .selectAll("rect")
      .data(vis.data_bins)
      .enter()
      .append("rect")
      .attr("class", "bar")
      .attr("id", (d, i) => vis.id + "_rect_" + i)
      .attr("y", (d) => vis.gH - vis.scY(d.length))
      .attr("width", w * 0.9)
      .attr("x", (d, i) => w * i)
      .style(
        "transform",
        (d) => `translate( 0, -${vis.gH - vis.scY(d.length)}px) `
      )
      .on("mouseover", vis.handleMouseOver)
      .on("mouseout", vis.handleMouseOut)
      .transition()
      .delay((d, i) => i * 10)
      .duration(750)
      .attr("height", (d) => vis.scY(d.length));

    group
      .selectAll("text")
      .data(vis.data_bins)
      .enter()
      .append("text")
      .attr("fill", "black")
      .attr("id", (d, i) => vis.id + "_label_" + i)
      .attr("x", (d, i) => w * (0.5 + i))
      .attr("text-anchor", "middle")
      .attr("dy", "-1em")
      .style("transform", (d) => `scale( 1, -1 )`)
      .text((d) => d.length)
      .transition()
      .delay((d, i) => i * 10)
      .duration(750)
      .attr("y", (d) => -vis.scY(d.length));
  }

  static _getLabel(id) {
    let segs = id.split("_rect_");
    let label_id = [segs[0], "_label_", segs[1]].join("");
    return document.getElementById(label_id);
  }

  handleMouseOver(e, d, i) {
    this.classList.remove("bar");
    this.classList.add("bar_hover");

    let label = Histogram._getLabel(this.id).classList.add("hover");
  }

  handleMouseOut(e, d, i) {
    this.classList.remove("bar_hover");
    this.classList.add("bar");
    let label = Histogram._getLabel(this.id).classList.remove("hover");
  }
}
export default Histogram;
