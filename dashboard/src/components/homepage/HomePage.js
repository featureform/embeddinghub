import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import { connect } from "react-redux";
import SearchBar from "../search/SearchBar";
import TilePanel from "../tilelinks/TilePanel";
import Paper from "@material-ui/core/Paper";
import Typography from "@material-ui/core/Typography";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(0),
    alignItems: "center",
  },
  search: {
    padding: theme.spacing(2),
  },
  searchTitle: {
    width: "100%",
    display: "flex",
    flexWrap: "wrap",
    justifyContent: "center",
    padding: theme.spacing(0.5),
    marginBottom: theme.spacing(2),
  },
  title: {
    marginBottom: theme.spacing(1),
  },
  sections: {
    padding: theme.spacing(0),
  },
  tilePanel: {
    padding: theme.spacing(2),
  },
}));

const HomePage = ({ sections }) => {
  let classes = useStyles();

  return (
    <div className={classes.root}>
      <div className={classes.search}>
        <div className={classes.searchTitle}>
          <img
            src="/Featureform_logo_pink.svg"
            height={150}
            width={150}
            alt="Featureform"
          />
        </div>
        <SearchBar homePage={true} />
      </div>
      <div className={classes.sections}>
        <div className={classes.tilePanel}>
          <div className={classes.title}>
            <Typography variant="h5"></Typography>
          </div>
          <Paper elevation={2}>
            <TilePanel sections={sections["features"]} />
          </Paper>
        </div>
      </div>
    </div>
  );
};

const mapStateToProps = (state) => ({
  sections: state.homePageSections,
});

export default connect(mapStateToProps)(HomePage);
