import React, { useEffect } from "react";
import { connect } from "react-redux";
import { fetchEntity } from "./EntityPageSlice.js";
import EntityPageView from "./EntityPageView.js";
import Loader from "react-loader-spinner";
import Container from "@material-ui/core/Container";
import Paper from "@material-ui/core/Paper";
import { setVariant } from "../resource-list/VariantSlice.js";
import NotFoundPage from "../notfoundpage/NotFoundPage";
import Resource from "../../api/resources/Resource.js";

const mapDispatchToProps = (dispatch) => {
  return {
    fetch: (api, type, title) => dispatch(fetchEntity({ api, type, title })),
    setVariant: (type, name, variant) =>
      dispatch(setVariant({ type, name, variant })),
  };
};

function mapStateToProps(state) {
  return {
    entityPage: state.entityPage,
    activeVariants: state.selectedVariant,
  };
}

const LoadingDots = () => {
  return (
    <div data-testid='loadingDotsId'>
    <Container maxWidth="xl">
      <Paper elevation={3}>
        <Container maxWidth="sm">
          <Loader type="ThreeDots" color="grey" height={40} width={40} />
        </Container>
      </Paper>
    </Container>
    </div>
  );
};

const fetchNotFound = (object) => {
  return !object?.resources?.name && !object?.resources?.type
}

const EntityPage = ({ api, entityPage, activeVariants, type, entity, ...props }) => {
  let resourceType = Resource[Resource.pathToType[type]];
  const fetchEntity = props.fetch;

  useEffect(async () => {
    if (api && type && entity) {
      await fetchEntity(api, type, entity);
    }
  }, [type, entity]);

  return (
    <div>
      {entityPage.loading ? (
        <LoadingDots />
      ) : entityPage.failed || (!entityPage.loading && fetchNotFound(entityPage)) ? (
        <NotFoundPage />
      ) : (
        <EntityPageView
          entity={entityPage}
          setVariant={props.setVariant}
          activeVariants={activeVariants}
          typePath={type}
          resourceType={resourceType}
        />
      )}
    </div>
  );
};

export default connect(mapStateToProps, mapDispatchToProps)(EntityPage);
