// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import ArrowDropUpIcon from '@mui/icons-material/ArrowDropUp';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import {
  Alert,
  Box,
  Button,
  FormControl,
  IconButton,
  InputAdornment,
  InputLabel,
  Popover,
  Slide,
  Snackbar,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import { styled } from '@mui/system';
import React, { useEffect, useRef, useState } from 'react';
import { connect } from 'react-redux';
import VariantView from './VariantView';

const StyledFormControl = styled(FormControl)(() => ({
  minWidth: 250,
}));

function mapStateToProps(state) {
  return {
    entityPage: state.entityPage,
    activeVariants: state.selectedVariant,
  };
}

export const IsAutoVariant = (name = '') => {
  let result = false;
  //current pattern "<optional_prefix>2024-04-05t07-05-18"
  let autoVariantPattern = /(?<date>\d{4}-\d{2}-\d{2})t(?<time>\d{2}-\d{2}-\d{2})$/;
  let isMatch = name.match(autoVariantPattern);
  if (isMatch) {
    const matchDate = isMatch.groups?.date;
    const matchTime = isMatch.groups?.time?.replace('-', ':');
    const propDate = new Date(matchDate + ' ' + matchTime);
    if (!isNaN(propDate.getTime())) {
      result = true;
    }
  }
  return result;
};

const VariantControl = ({
  variant,
  variantListProp = [],
  resources,
  handleVariantChange,
}) => {
  const ref = useRef();

  const [open, setOpen] = useState(false);
  const [openVarDrop, setOpenVarDrop] = useState(false);
  const [variantList, setVariantList] = useState([]);
  const [filteredVarList, setFilteredVarList] = useState([]);

  const isChecked = !IsAutoVariant(variant);

  const closeSnackBar = (_, reason) => {
    if (reason === 'clickaway') {
      return;
    }
    setOpen(false);
  };

  const copyToClipBoard = (variantName = '') => {
    if (variantName) {
      navigator.clipboard.writeText(variantName);
      setOpen(true);
    }
  };

  function transition(props) {
    return <Slide {...props} direction='right' />;
  }

  const createVariantViewItem = (propItem = {}, id = 0) => {
    let result = {
      id: id,
      name: '---',
      variant: propItem.variant,
      owner: propItem.owner,
      status: propItem.status,
      created: new Date(),
    };
    const propDate = new Date(propItem.created);
    if (!isNaN(propDate.getTime())) {
      //is valid
      result.created = propDate;
    } else {
      console.error(
        `error on (${propItem?.name} - ${propItem?.variant}) created field`,
        propItem?.created
      );
    }
    return result;
  };

  useEffect(() => {
    let tempList = [];
    if (variantListProp.length && resources?.variants) {
      tempList = variantListProp.map((vr, index) => {
        const foundRecord = resources.variants[vr];
        return createVariantViewItem(foundRecord, index);
      });
      tempList = tempList.sort((a, b) => b.created - a.created);
    }
    setVariantList(tempList);
  }, [variantListProp]);

  const handleSelect = (variantName = '') => {
    if (variantName) {
      handleVariantChange(variantName);
      setOpenVarDrop(false);
    }
  };

  const handleSearch = (searchText = '', namedOnly = true) => {
    let filteredList = variantList.filter((vr) => {
      return vr.variant?.includes(searchText);
    });

    if (namedOnly) {
      filteredList = filteredList.filter(filterNamedOnly);
    }

    setFilteredVarList(filteredList);
  };

  const handleClose = () => {
    setOpenVarDrop(false);
  };

  const handleOpen = () => {
    //reset the filtered list. initially start with named only
    setFilteredVarList(variantList);
    setOpenVarDrop(true);
  };

  function filterNamedOnly(vr) {
    return !IsAutoVariant(vr.variant);
  }

  return (
    <>
      <Snackbar
        open={open}
        autoHideDuration={1250}
        onClose={closeSnackBar}
        TransitionComponent={transition}
      >
        <Alert severity='success' onClose={closeSnackBar}>
          <Typography>Copied to clipboard!</Typography>
        </Alert>
      </Snackbar>

      <Box style={{ minWidth: 325 }}>
        <StyledFormControl>
          <InputLabel shrink id='variantControlId'>
            Variant
          </InputLabel>
          <TextField
            value={variant}
            label='Variant'
            onChange={handleSelect}
            onClick={handleOpen}
            type='button'
            ref={ref}
            InputProps={{
              endAdornment: (
                <InputAdornment position='end'>
                  <IconButton>
                    {openVarDrop ? <ArrowDropUpIcon /> : <ArrowDropDownIcon />}
                  </IconButton>
                </InputAdornment>
              ),
            }}
            inputProps={{
              'aria-label': 'searchVariant',
              'data-testid': 'variantControlSelectId',
            }}
          />
        </StyledFormControl>
        <Tooltip title='Copy to Clipboard'>
          <Button
            role='none'
            onClick={() => copyToClipBoard(variant)}
            style={{ marginTop: 10 }}
          >
            <ContentCopyIcon fontSize='medium' />
          </Button>
        </Tooltip>
        <Popover
          style={{ marginTop: '0.25em' }}
          open={openVarDrop}
          anchorReference='anchorEl'
          onClose={handleClose}
          anchorEl={ref?.current}
          anchorOrigin={{
            vertical: 'bottom',
            horizontal: 'left',
          }}
        >
          <VariantView
            variantList={filteredVarList}
            handleClose={handleClose}
            handleSelect={handleSelect}
            handleSearch={handleSearch}
            initialChecked={isChecked}
          />
        </Popover>
      </Box>
    </>
  );
};

export default connect(mapStateToProps)(VariantControl);
