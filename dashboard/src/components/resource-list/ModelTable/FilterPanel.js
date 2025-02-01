import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import {
  StyledAccordion, 
  filterItemTxtSize, 
  StyledFormControlLabel,
  StyledDivider,
} from '../BaseFilterPanel';
import {
  AccordionDetails,
  AccordionSummary,
  Checkbox,
  FormGroup,
  Typography,
} from '@mui/material';
import React, { useState } from 'react';

export const FilterPanel = ({
  filters,
  tags = [],
  onCheckBoxChange = () => null,
}) => {
  const [tagsExpanded, setTagsExpanded] = useState(false);

  return (
    <>
      <StyledAccordion
        expanded={tagsExpanded}
        onChange={() => setTagsExpanded(!tagsExpanded)}
        disableGutters={true}
      >
        <AccordionSummary expandIcon={<ArrowDropDownIcon />}>
          <Typography sx={{ fontSize: filterItemTxtSize }}>Tags</Typography>
        </AccordionSummary>
        <StyledDivider />
        <AccordionDetails>
          <FormGroup>
            {tags.map((tag) => (
              <StyledFormControlLabel
                key={tag.name}
                control={
                  <Checkbox
                    checked={filters.Tags?.includes(tag.name) || false}
                    onChange={() => onCheckBoxChange('Tags', tag.name)}
                    sx={{ '& .MuiSvgIcon-root': { fontSize: 25 } }}
                  />
                }
                label={tag.name}
              />
            ))}
          </FormGroup>
        </AccordionDetails>
      </StyledAccordion>
    </>
  );
};

export default FilterPanel;