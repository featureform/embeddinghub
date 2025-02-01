// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import CloseIcon from '@mui/icons-material/Close';
import {
  Box,
  FormControlLabel,
  IconButton,
  TextField,
  Typography,
} from '@mui/material';
import Checkbox from '@mui/material/Checkbox';
import { DataGrid } from '@mui/x-data-grid';
import React, { useEffect, useState } from 'react';

export default function VariantView({
  variantList = [],
  handleClose = () => null,
  handleSelect = () => null,
  handleSearch = () => null,
  initialChecked = false,
}) {
  const columns = [
    {
      field: 'id',
      headerName: 'id',
      flex: 1,
      width: 100,
      editable: false,
      sortable: false,
      filterable: false,
      hide: true,
    },
    {
      field: 'name',
      headerName: 'Name',
      flex: 1,
      width: 100,
      editable: false,
      sortable: false,
      filterable: false,
      hide: true,
    },
    {
      field: 'variant',
      headerName: 'Variant',
      flex: 1,
      width: 125,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'owner',
      headerName: 'Owner',
      flex: 1,
      width: 125,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'status',
      headerName: 'Status',
      flex: 1,
      width: 125,
      editable: false,
      sortable: false,
      filterable: false,
    },
  ];
  const ENTER_KEY = 'Enter';
  const [searchQuery, setSearchQuery] = useState('');
  const [checked, setChecked] = useState(initialChecked);

  // Sets the initial value for the variant checkbox 
  useEffect(() => {
    doSearch(searchQuery, checked);
  }, []);  

  const handleRowSelect = (selectedRow) => {
    if (selectedRow?.row) {
      handleSelect?.(selectedRow.row.variant);
    }
  };

  // doSearch and doCheckbox are subsequently fired when the user interacts with the search bar or checkbox
  const doSearch = (searchText = '', checkValue = checked) => {
    handleSearch(searchText, checkValue);
  };

  const doCheckBox = (event) => {
    const checkValue = event.target.checked;
    setChecked(checkValue);
    doSearch(searchQuery, checkValue);
  };

  return (
    <Box data-testid='variantViewId'>
      <Box sx={{ margin: '1em', width: 650 }}>
        <Box sx={{ margin: 1 }}>
          <Typography align='center'>Variants</Typography>
          <IconButton
            size='large'
            onClick={() => handleClose?.()}
            sx={{ position: 'absolute', right: '10px', top: '5px' }}
          >
            <CloseIcon />
          </IconButton>
        </Box>
        <TextField
          variant='outlined'
          margin='normal'
          autoFocus
          fullWidth
          label='Search Variants'
          name='searchVariants'
          inputProps={{
            'aria-label': 'search variant input',
            'data-testid': 'searchVariantInputId',
          }}
          onChange={(event) => {
            const rawText = event.target.value;
            if (rawText === '') {
              // user is deleting the text field. allow this and clear out state
              setSearchQuery(rawText);
              doSearch('');
              return;
            }
            const searchText = event.target.value ?? '';
            if (searchText.trim()) {
              setSearchQuery(searchText);
            }
          }}
          value={searchQuery}
          onKeyDown={(event) => {
            if (event.key === ENTER_KEY && searchQuery) {
              doSearch(searchQuery);
            }
          }}
        />
        <Box>
          <FormControlLabel
            control={<Checkbox checked={checked} onChange={doCheckBox} />}
            label='Named Variants Only'
          />
        </Box>
      </Box>
      <Box sx={{ margin: 1 }}>
        <DataGrid
          disableVirtualization
          density='compact'
          autoHeight
          sx={{
            '& .MuiDataGrid-cell:focus': {
              outline: 'none',
            },
            '& .MuiDataGrid-columnHeader:focus': {
              outline: 'none',
            },
            '& .MuiDataGrid-columnHeaderTitle': {
              fontWeight: 'bold',
            },
            marginBottom: '1.5em',
            cursor: 'pointer',
          }}
          aria-label='Other Runs'
          rows={variantList}
          onRowClick={handleRowSelect}
          columns={columns}
          initialState={{
            pagination: { paginationModel: { page: 0, pageSize: 15 } },
          }}
          pageSize={15}
          rowsPerPageOptions={[15]}
          disableColumnMenu
          disableColumnSelector
        />
      </Box>
    </Box>
  );
}
