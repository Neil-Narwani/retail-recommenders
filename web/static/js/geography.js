import Histogram from "./d3/Histogram.js";

const padZipCode = (numeric) => {
  var stringZip = "0000" + numeric; // will never have more than 4 leading 0s
  return stringZip.substring(stringZip.length - 5);
};

const handleZipHistogramForm = ($form) => {
  const limit = $form.querySelector("#limit").value;
  return limit;
};

const handleFetchError = (err) => console.error(err);

const fetchData = async (limit) =>
  d3.json("/api/geography/zip", {
    method: "POST",
    headers: {
      "Content-type": "application/json",
    },
    body: JSON.stringify({
      limit,
    }),
  });

const dataMapper = (data) =>
  data.customers
    .map((customer) => customer.zip.substr(0, 5))
    .filter((zip) => zip.length === 5 && !isNaN(parseInt(zip, 10), 10));

const initZipHistogram = (data) => {
  let zipHistogram = new Histogram("purchasesByZip", dataMapper(data), {
    xLabel: "Zip Code",
    yLabel: "Number of Purchases",
    xFormat: (xLabel) => padZipCode(String(xLabel)),
  });

  const $form = document.querySelector("#purchasesByZipForm");
  $form.addEventListener("submit", (e) => {
    e.preventDefault();
    const limit = handleZipHistogramForm($form);
    fetchData(limit)
      .then((data) => {
        zipHistogram.resetWithNewData(dataMapper(data));
      })
      .catch(handleFetchError);
  });
};

(() => {
  fetchData().then(initZipHistogram).catch(handleFetchError);
})();
