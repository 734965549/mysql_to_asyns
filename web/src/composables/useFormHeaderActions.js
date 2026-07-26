import { shallowRef } from "vue";

/** Bridge so MainLayout header can render form Cancel/Submit without Pinia. */
export const formHeaderActions = shallowRef(null);

export function setFormHeaderActions(actions) {
  formHeaderActions.value = actions;
}

export function clearFormHeaderActions() {
  formHeaderActions.value = null;
}
