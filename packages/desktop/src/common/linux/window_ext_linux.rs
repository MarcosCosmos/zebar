use anyhow::bail;
use gtk::prelude::{GtkWindowExt, WidgetExt};
use gtk_layer_shell::{Edge, LayerShell};
use tauri::{PhysicalSize, Runtime, WebviewWindow, Window};
use crate::widget_pack::DockEdge;

pub trait WindowExtLinux {
    fn allocate_app_bar(
        &self,
        size: PhysicalSize<i32>,
        thickness: i32,
        edge: DockEdge,
    ) -> anyhow::Result<()>;
}

impl<R: Runtime> WindowExtLinux for WebviewWindow<R> {
    fn allocate_app_bar(
        &self,
        size: PhysicalSize<i32>,
        thickness: i32,
        edge: DockEdge,
    ) -> anyhow::Result<()> {
        let gtk_window = self.gtk_window()?;

        gtk_window.set_layer(gtk_layer_shell::Layer::Bottom);
        gtk_window.auto_exclusive_zone_enable();
        gtk_window.set_skip_pager_hint(true);
        gtk_window.set_deletable(false);
        gtk_window.set_resizable(false);
        gtk_window.set_app_paintable(true);
        gtk_window.set_decorated(false);
        gtk_window.stick();


        // TODO: replace this value with some parameter. Maybe a thickness
        // value still? It will be the margin config option.
        gtk_window.set_exclusive_zone(thickness);
        // TODO: Add console info logging.

        let gtk_anchor = match edge {
            DockEdge::Top => Edge::Top,
            DockEdge::Bottom => Edge::Bottom,
            DockEdge::Left => Edge::Left,
            DockEdge::Right => Edge::Right,
        };

        gtk_window.set_anchor(gtk_anchor, true);
        gtk_window.set_layer_shell_margin(gtk_anchor, thickness.abs());

        gtk_window.set_size_request(size.width, size.height);

        gtk_window.show_all();

        Ok(())
    }
}