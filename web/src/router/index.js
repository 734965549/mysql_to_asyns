import { createRouter, createWebHashHistory } from "vue-router";
import MainLayout from "../layouts/MainLayout.vue";
import TaskListView from "../views/TaskListView.vue";

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    {
      path: "/task-detail/:id",
      name: "task-detail",
      component: () => import("../views/TaskDetailView.vue"),
      meta: { standalone: true },
    },
    {
      path: "/",
      component: MainLayout,
      children: [
        { path: "", redirect: "/tasks" },
        {
          path: "tasks",
          name: "tasks",
          component: TaskListView,
        },
        {
          path: "tasks/new/select",
          name: "task-select-type",
          component: () => import("../views/TaskSelectTypeView.vue"),
        },
        {
          path: "tasks/new",
          name: "task-create",
          component: () => import("../views/TaskFormView.vue"),
          meta: { mode: "create" },
        },
        {
          path: "tasks/new/config",
          name: "task-create-config",
          component: () => import("../views/TaskFormView.vue"),
          meta: { mode: "create" },
        },
        {
          path: "tasks/:id/edit",
          name: "task-edit",
          component: () => import("../views/TaskFormView.vue"),
          meta: { mode: "edit" },
        },
        {
          path: "config",
          name: "config",
          component: () => import("../views/ConfigView.vue"),
        },
      ],
    },
  ],
});

export default router;
