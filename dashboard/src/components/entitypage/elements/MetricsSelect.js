// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Box, FormControl, InputLabel, MenuItem, Select } from '@mui/material';
import Typography from '@mui/material/Typography';
import React, { useEffect, useState } from 'react';
import { connect } from 'react-redux';
import MetricsAPI from '../../api/resources/Metrics.js';
import {
  fetchMetrics,
  modifyInstances,
  modifyMetrics,
} from './MetricsSelectSlice.js';

const metricsAPI = new MetricsAPI();

const mapDispatchToProps = (dispatch) => {
  return {
    fetchMetrics: (api) => dispatch(fetchMetrics({ api })),
    modifyMetrics: (selection) => dispatch(modifyMetrics({ selection })),
    setInstances: (instances) => dispatch(modifyInstances({ instances })),
  };
};

function mapStateToProps(state) {
  return {
    metricsSelect: state.metricsSelect,
  };
}

function MetricsSelect({ metricsSelect, modifyMetrics, fetchMetrics }) {
  const [selection, setSelection] = useState('');
  const options = metricsSelect.resources ? metricsSelect.resources : {};
  const metrics = Object.keys(options);
  const metricsDesc =
    options && selection !== '' ? options[selection][0]['help'] : '';

  useEffect(() => {
    fetchMetrics(metricsAPI);
  }, [fetchMetrics]);

  const handleChange = (event) => {
    modifyMetrics(event.target.value);
    if (selection !== event.target.value) {
      setSelection(event.target.value);
    }
  };

  return (
    <div>
      <div>
        <Box sx={{ minWidth: 240 }}>
          <FormControl fullWidth>
            <InputLabel id='demo-simple-select-label'>Metric Select</InputLabel>
            <Select
              labelId='demo-simple-select-label'
              id='demo-simple-select'
              value={selection}
              label='Metrics Options'
              onChange={handleChange}
            >
              {metrics.map((option) => (
                <MenuItem key={option} value={option}>
                  {option}
                </MenuItem>
              ))}
            </Select>
            <Typography>{metricsDesc}</Typography>
          </FormControl>
        </Box>
      </div>
    </div>
  );
}

export default connect(mapStateToProps, mapDispatchToProps)(MetricsSelect);
