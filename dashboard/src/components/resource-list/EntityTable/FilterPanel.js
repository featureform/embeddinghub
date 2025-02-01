import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import {
  StyledAccordion, 
  filterItemTxtSize, 
  StyledFormControlLabel,
  StyledDivider,} from '../BaseFilterPanel';
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
  labels = [],
  features = [],
  onCheckBoxChange = () => null,
}) => {
  const [tagsExpanded, setTagsExpanded] = useState(false);
  const [labelsExpanded, setLabelsExpanded] = useState(false);
  const [featuresExpanded, setFeaturesExpanded] = useState(false);
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

      <StyledAccordion
        expanded={labelsExpanded}
        onChange={() => setLabelsExpanded(!labelsExpanded)}
        disableGutters={true}
      >
        <AccordionSummary expandIcon={<ArrowDropDownIcon />}>
          <Typography sx={{ fontSize: filterItemTxtSize }}>Labels</Typography>
        </AccordionSummary>
        <StyledDivider />
        <AccordionDetails>
          <FormGroup>
            {labels.map((label) => (
              <StyledFormControlLabel
                key={label}
                control={
                  <Checkbox
                    checked={filters.Labels?.includes(label) || false}
                    onChange={() => onCheckBoxChange('Labels', label)}
                    sx={{ '& .MuiSvgIcon-root': { fontSize: 25 } }}
                  />
                }
                label={label}
              />
            ))}
          </FormGroup>
        </AccordionDetails>
      </StyledAccordion>

      <StyledAccordion
        expanded={featuresExpanded}
        onChange={() => setFeaturesExpanded(!featuresExpanded)}
        disableGutters={true}
      >
        <AccordionSummary expandIcon={<ArrowDropDownIcon />}>
          <Typography sx={{ fontSize: filterItemTxtSize }}>Features</Typography>
        </AccordionSummary>
        <StyledDivider />
        <AccordionDetails>
        <FormGroup>
            {features.map((feature) => (
              <StyledFormControlLabel
                key={feature}
                control={
                  <Checkbox
                    checked={filters.Features?.includes(feature) || false}
                    onChange={() => onCheckBoxChange('Features', feature)}
                    sx={{ '& .MuiSvgIcon-root': { fontSize: 25 } }}
                  />
                }
                label={feature}
              />
            ))}
          </FormGroup>
        </AccordionDetails>
      </StyledAccordion>
    </>
  );
};

export default FilterPanel;
