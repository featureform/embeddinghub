import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";

export const fetchEntity = createAsyncThunk(
  "entityPage/fetchByTitle",
  async ({ api, type, title }, { signal }) => {
    const response = await api.fetchEntity(type, title, signal);
    return response;
  },
  {
    condition: ({ api, type, title }, { getState }) => {},
  }
);

const entityPageSlice = createSlice({
  name: "entityPage",
  // initialState is a map between each resource type to an empty object.
  initialState: {},
  extraReducers: {
    [fetchEntity.pending]: (state, action) => {
      const requestId = action.meta.requestId;
      state.requestId = requestId;
      state.resources = null;
      state.loading = true;
      state.failed = false;
    },
    [fetchEntity.fulfilled]: (state, action) => {
      const requestId = action.meta.requestId;
      if (requestId !== state.requestId) {
        return;
      }
      state.resources = action.payload?.data;
      state.loading = false;
      state.failed = false;
    },
    [fetchEntity.rejected]: (state, action) => {
      const requestId = action.meta.requestId;
      if (requestId !== state.requestId) {
        return;
      }
      state.loading = false;
      state.failed = true;
    },
  },
});

export default entityPageSlice.reducer;
