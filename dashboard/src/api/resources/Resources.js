import Resource from "api/resources/Resource.js";

export const testData = [
  {
    name: "User sample preferences",
    "default-variant": "first-variant",
    type: "Feature",
    "all-variants": ["first-variant", "normalized variant"],
    variants: {
      "first-variant": {
        "variant-name": "first-variant",
        dimensions: 3,
        created: "2020-08-09-0290499",
        owner: "Simba Khadder",
        visibility: "private",
        revision: "2020-08-10-39402409",
        tags: ["model2vec", "compressed"],
        description: "Vector generated based on user preferences",
      },
      "normalized variant": {
        "variant-name": "normalized variant",
        dimensions: 3,
        created: "2020-08-09-0290499",
        owner: "Simba Khadder",
        visibility: "private",
        revision: "2020-08-10-39402409",
        tags: ["model2vec", "compressed"],
        description: "Vector generated based on user preferences, normalized",
      },
    },
  },
];

export const providerLogos = Object.freeze({
  Redis: "/Redis_Logo.svg",
  BigQuery: "/google_bigquery-ar21.svg",
  "Apache Spark": "/Apache_Spark_logo.svg",
});

const API_URL = "http://localhost:8181";
const local = false;

export default class ResourcesAPI {
  checkStatus() {
    return fetch(API_URL, {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => {
        res.json().then((json_data) => ({ data: json_data }));
      })
      .catch((error) => {
        console.error(error);
      });
  }

  fetchResources(type) {
    var fetchAddress;
    let resourceType = Resource[type];
    if (local) {
      fetchAddress = `/data/lists/wine-data.json`;
    } else {
      fetchAddress = `${API_URL}${resourceType.urlPath}`;
    }
    if (process.env.REACT_APP_EMPTY_RESOURCE_VIEW === "true") {
      fetchAddress = "/data/lists/wine-data-empty.json";
    }
    return fetch(fetchAddress, {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) =>
        res.json().then((json_data) => {
          if (local) {
            return { data: json_data[type] };
          } else {
            return { data: json_data };
          }
        })
      )
      .catch((error) => {
        console.error(error);
      });
  }

  fetchEntity(type, title) {
    var fetchAddress;
    if (local) {
      fetchAddress = "/data/" + type + "/" + title + ".json";
    } else {
      fetchAddress = `${API_URL}/${type}/${title}`;
    }

    return fetch(fetchAddress, {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) =>
        res.json().then((json_data) => {
          return { data: json_data };
        })
      )
      .catch((error) => {
        console.error(error);
      });
  }

  fetchSearch(query) {
    const TypesenseClient = require("./Search.js");
    const myTypesenseClient = new TypesenseClient("8108", "localhost", "xyz");
    const myResults = myTypesenseClient.search(
      { q: query, query_by: "Name" },
      "resource"
    );

    return myResults.then((results) => {
      return results.results();
    });
  }

  fetchVariantSearchStub(query) {
    const fetchAddress = "/data/lists/search_results_example.json";

    return fetch(fetchAddress, {
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => res.json().then((json_data) => ({ data: json_data })))
      .catch((error) => {
        console.error(error);
      });
  }
}
