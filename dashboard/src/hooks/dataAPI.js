// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

let hostname = 'localhost';
let port = 3000;
if (typeof window !== 'undefined') {
  hostname = window.location.hostname;
  port = window.location.port;
}
var API_URL = '//' + hostname + ':' + port;
if (process.env.REACT_APP_API_URL) {
  API_URL = process.env.REACT_APP_API_URL.trim();
}

//if you want to override the api url (in any environment local to your machine, set this in ".env.local")
//.env.local is not tracked in source
if (process.env.NEXT_PUBLIC_REACT_APP_API_URL) {
  API_URL = process.env.NEXT_PUBLIC_REACT_APP_API_URL.trim();
}

export function useDataAPI() {
  const getTags = async (type = '', resourceName = '', variant = '') => {
    const address = `${API_URL}/data/${type}/${resourceName}/gettags`;
    const result = await fetch(address, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ variant: variant }),
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error(error);
        return error;
      });
    return result;
  };

  const postTags = async (
    type = '',
    resourceName = '',
    variant = '',
    tagList = []
  ) => {
    const address = `${API_URL}/data/${type}/${resourceName}/tags`;
    const result = await fetch(address, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ tags: tagList, variant: variant }),
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error(error);
        return error;
      });
    return result;
  };

  const getTaskRuns = async (searchParams = {}) => {
    const result = await fetch(`${API_URL}/data/taskruns`, {
      cache: 'no-store',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(searchParams),
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error('Error fetching tasks from server: ', error);

        return [];
      });

    return result;
  };

  const getTaskRunDetails = async (taskId = '', taskRunId = '') => {
    const result = await fetch(
      `${API_URL}/data/taskruns/taskrundetail/${taskId}/${taskRunId}`,
      {
        cache: 'no-store',
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      }
    )
      .then((res) => res.json())
      .catch((error) => {
        console.error('Error fetching tasks from server: ', error);

        return [];
      });

    return result;
  };

  const getTypeTags = async (resourceType = '') => {
    const result = await fetch(`${API_URL}/data/${resourceType}/prop/tags`, {
      cache: 'no-store',
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error('Error fetching tags list from server: ', error);

        return [];
      });

    return result;
  };

  const getTypeOwners = async (resourceType = '') => {
    const result = await fetch(`${API_URL}/data/${resourceType}/prop/owners`, {
      cache: 'no-store',
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error('Error fetching owners list from server: ', error);

        return [];
      });

    return result;
  };

  const getProviders = async (filters = {}) => {
    const result = await fetch(`${API_URL}/data/providers`, {
      cache: 'no-store',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(filters),
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error('Error fetching providers from server: ', error);

        return [];
      });

    return result;
  };

  const getFeatureVariants = async (filters = {}) => {
    const result = await fetch(`${API_URL}/data/feature/variants`, {
      cache: 'no-store',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(filters),
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error('Error fetching feature variants from server: ', error);

        return [];
      });

    return result;
  };

  const getSourceVariants = async (filters = {}) => {
    const result = await fetch(`${API_URL}/data/datasets/variants`, {
      cache: 'no-store',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(filters),
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error('Error fetching dataset variants from server: ', error);

        return [];
      });

    return result;
  };

  const getLabelVariants = async (filters = {}) => {
    const result = await fetch(`${API_URL}/data/label/variants`, {
      cache: 'no-store',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(filters),
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error('Error fetching dataset variants from server: ', error);

        return [];
      });

    return result;
  };

  const getTrainingSetVariants = async (filters = {}) => {
    const result = await fetch(
      `${API_URL}/data/training-sets/variants`,
      {
        cache: 'no-store',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(filters),
      }
    )
      .then((res) => res.json())
      .catch((error) => {
        console.error(
          'Error fetching trainingsets variants from server: ',
          error
        );

        return [];
      });

    return result;
  };
  const getEntities = async (filters = {}) => {
    const result = await fetch(`${API_URL}/data/entities`, {
      cache: 'no-store',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(filters),
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error('Error fetching entities from server: ', error);

        return [];
      });

    return result;
  };

  const getModels = async (filters = {}) => {
    const result = await fetch(
      `${API_URL}/data/models`, 
      {
      cache: 'no-store',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(filters),
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error('Error fetching models from server: ', error);

        return [];
      });

    return result;
  };

  const searchResources = async (query) => {
    const result = await fetch(
      `${API_URL}/data/search?q=${query}`, 
      {
      cache: 'no-store',
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    })
      .then((res) => res.json())
      .catch((error) => {
        console.error('Error fetching search results from server: ', error);

        return [];
      });

    return result;
  };
        
  return {
    getTags,
    postTags,
    getTaskRuns,
    getTaskRunDetails,
    getTypeTags,
    getProviders,
    getFeatureVariants,
    getSourceVariants,
    getTypeOwners,
    getLabelVariants,
    getTrainingSetVariants,
    getEntities,
    searchResources,
    getModels,
  };
}
